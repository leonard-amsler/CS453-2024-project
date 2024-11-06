/**
 * @file   tm.c
 * @author [...]
 *
 * @section LICENSE
 *
 * [...]
 *
 * @section DESCRIPTION
 *
 * Implementation of the transaction manager using dual-versioned memory.
**/

#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>
#include <pthread.h>
#include <tm.h>
#include "macros.h"

// Constants for the access set
static const long ACC_SET_NONE = -1;
static const long ACC_SET_MULTIPLE = -2;

// Batcher data with synchronization primitives
typedef struct blocked_thread_node {
    pthread_t thread;                       // Thread identifier
    struct blocked_thread_node* next;       // Next node in the list
} blocked_thread_node_t;

// Batcher data with synchronization primitives
typedef struct {
    size_t epoch;                           // Current epoch
    size_t remaining;                       // Remaining transactions in the current epoch
    blocked_thread_node_t* blocked_head;    // Head of the blocked threads list
    size_t blocked_count;                   // Number of blocked threads
    pthread_mutex_t batcher_mutex;          // Mutex to protect batcher data
    pthread_cond_t batcher_cond_wake_up;    // Condition variable to wake up threads that should be unblocked
    pthread_cond_t batcher_cond_enter;      // Condition variable to wake up threads that should enter
    bool wake_up_threads_only;              // Whether to wake up threads only
} batcher_data;

// Control structure for each word
typedef struct {
    bool written_while_epoch;               // Whether the word was written in the current epoch
    long access_set;                        // Pointer to the transaction object, or special constants (ACC_SET_NONE, ACC_SET_MULTIPLE)
    pthread_mutex_t word_mutex;             // Mutex to protect the word
} controls;

// Dual segment structure
typedef struct dual_segment {
    uint8_t* ro_words;                      // Read-only copy (array of bytes)
    uint8_t* rw_words;                      // Writable copy (array of bytes)
    controls* controls;                     // Control structures per word (array of controls)
    size_t size;                            // Number of words in a segment
    bool can_be_freed;                      // Whether the segment can be freed (Only the first segment cannot be freed)
    struct dual_segment* next;              // Pointer to the next segment
} dual_segment;

// Shared memory region structure
typedef struct region {
    dual_segment* segment_head;             // Pointer to the first segment
    size_t align;                           // Number of bytes per word
    batcher_data* batcher;                  // Batcher data for epoch management
    pthread_mutex_t segments_mutex;         // Mutex to protect the segments list
} region;

// Transaction structure
struct transaction {
    bool is_ro;                             // Whether the transaction is read-only
};

/** Create a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
**/
shared_t tm_create(size_t size, size_t align) {
    // Verify the size and alignment parameters
    if (size == 0 || align == 0 || size % align != 0 || (align & (align - 1)) != 0) {
        printf("Invalid size or alignment\n");
        return invalid_shared;
    }

    // Allocate the shared memory region
    struct region* region = malloc(sizeof(struct region));
    if (!region) {
        printf("Memory allocation failed for the region\n");
        return invalid_shared;
    }

    // Initialize the region properties
    region->segment_head = NULL;
    region->align = align;
    if (pthread_mutex_init(&(region->segments_mutex), NULL) != 0) {
        printf("Mutex initialization failed for the segments_mutex\n");
        free(region);
        return invalid_shared;
    }

    // Allocate and initialize the first segment
    dual_segment* segment = malloc(sizeof(dual_segment));
    if (!segment) {
        printf("Memory allocation failed for the dual segment\n");
        pthread_mutex_destroy(&(region->segments_mutex));
        free(region);
        return invalid_shared;
    }

    // Set can_be_freed to false for the first segment
    segment->can_be_freed = false;
    segment->next = NULL;

    // Calculate the number of words
    size_t word_size = align;
    size_t num_words = size / word_size;
    segment->size = num_words; // Number of words in the segment

    // Allocate ro_words
    if (posix_memalign((void**)&(segment->ro_words), align, size) != 0) {
        printf("Memory allocation failed for the read-only segment\n");
        free(segment);
        pthread_mutex_destroy(&(region->segments_mutex));
        free(region);
        return invalid_shared;
    }

    // Allocate rw_words
    if (posix_memalign((void**)&(segment->rw_words), align, size) != 0) {
        printf("Memory allocation failed for the read-write segment\n");
        free(segment->ro_words);
        free(segment);
        pthread_mutex_destroy(&(region->segments_mutex));
        free(region);
        return invalid_shared;
    }

    // Initialize ro_words and rw_words to zero
    memset(segment->ro_words, 0, size);
    memset(segment->rw_words, 0, size);

    // Allocate controls array
    segment->controls = malloc(num_words * sizeof(controls));
    if (!segment->controls) {
        printf("Memory allocation failed for the controls\n");
        free(segment->rw_words);
        free(segment->ro_words);
        free(segment);
        pthread_mutex_destroy(&(region->segments_mutex));
        free(region);
        return invalid_shared;
    }

    // Initialize the controls array
    for (size_t i = 0; i < num_words; i++) {
        segment->controls[i].written_while_epoch = false;
        segment->controls[i].access_set = ACC_SET_NONE;
        if (pthread_mutex_init(&(segment->controls[i].word_mutex), NULL) != 0) {
            printf("Mutex initialization failed for word_mutex\n");
            // Clean up resources allocated so far
            for (size_t j = 0; j < i; j++) {
                pthread_mutex_destroy(&(segment->controls[j].word_mutex));
            }
            free(segment->controls);
            free(segment->rw_words);
            free(segment->ro_words);
            free(segment);
            pthread_mutex_destroy(&(region->segments_mutex));
            free(region);
            return invalid_shared;
        }
    }

    // Add the segment to the region's segment list
    region->segment_head = segment;

    // Allocate and initialize the batcher data
    region->batcher = malloc(sizeof(batcher_data));
    if (!region->batcher) {
        printf("Memory allocation failed for the batcher\n");
        // Clean up resources
        for (size_t i = 0; i < num_words; i++) {
            pthread_mutex_destroy(&(segment->controls[i].word_mutex));
        }
        free(segment->controls);
        free(segment->rw_words);
        free(segment->ro_words);
        free(segment);
        pthread_mutex_destroy(&(region->segments_mutex));
        free(region);
        return invalid_shared;
    }

    region->batcher->epoch = 0;
    region->batcher->remaining = 0;
    region->batcher->blocked_head = NULL;
    region->batcher->blocked_count = 0;
    region->batcher->wake_up_threads_only = false;

    if (pthread_mutex_init(&(region->batcher->batcher_mutex), NULL) != 0) {
        printf("Mutex initialization failed for batcher_mutex\n");
        free(region->batcher);
        // Clean up resources
        for (size_t i = 0; i < num_words; i++) {
            pthread_mutex_destroy(&(segment->controls[i].word_mutex));
        }
        free(segment->controls);
        free(segment->rw_words);
        free(segment->ro_words);
        free(segment);
        pthread_mutex_destroy(&(region->segments_mutex));
        free(region);
        return invalid_shared;
    }

    if (pthread_cond_init(&(region->batcher->batcher_cond_enter), NULL) != 0) {
        printf("Condition variable initialization failed for batcher_cond_enter\n");
        pthread_mutex_destroy(&(region->batcher->batcher_mutex));
        free(region->batcher);
        // Clean up resources
        for (size_t i = 0; i < num_words; i++) {
            pthread_mutex_destroy(&(segment->controls[i].word_mutex));
        }
        free(segment->controls);
        free(segment->rw_words);
        free(segment->ro_words);
        free(segment);
        pthread_mutex_destroy(&(region->segments_mutex));
        free(region);
        return invalid_shared;
    }

    if (pthread_cond_init(&(region->batcher->batcher_cond_wake_up), NULL) != 0) {
        printf("Condition variable initialization failed for batcher_cond_wake_up\n");
        pthread_cond_destroy(&(region->batcher->batcher_cond_enter));
        pthread_mutex_destroy(&(region->batcher->batcher_mutex));
        free(region->batcher);
        // Clean up resources
        for (size_t i = 0; i < num_words; i++) {
            pthread_mutex_destroy(&(segment->controls[i].word_mutex));
        }
        free(segment->controls);
        free(segment->rw_words);
        free(segment->ro_words);
        free(segment);
        pthread_mutex_destroy(&(region->segments_mutex));
        free(region);
        return invalid_shared;
    }

    return (shared_t)region; // Return the initialized shared memory region
}


/** Destroy a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t shared) {
    struct region* region = (struct region*)shared;

    // Free all segments
    dual_segment* current = region->segment_head;
    while (current) {
        dual_segment* next = current->next;

        // Free the controls array
        for (size_t i = 0; i < current->size; i++) {
            pthread_mutex_destroy(&(current->controls[i].word_mutex));
        }
        free(current->controls);

        // Free the rw_words and ro_words
        free(current->rw_words);
        free(current->ro_words);

        // Free the segment itself
        free(current);

        current = next;
    }

    // Free the batcher data
    pthread_mutex_destroy(&(region->batcher->batcher_mutex));
    pthread_cond_destroy(&(region->batcher->batcher_cond_enter));
    pthread_cond_destroy(&(region->batcher->batcher_cond_wake_up));

    // Free the blocked threads list
    blocked_thread_node_t* current_blocked = region->batcher->blocked_head;
    while (current_blocked) {
        blocked_thread_node_t* next_blocked = current_blocked->next;
        free(current_blocked);
        current_blocked = next_blocked;
    }

    // Free the batcher
    free(region->batcher);

    // Free the region
    pthread_mutex_destroy(&(region->segments_mutex));
    free(region);
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t shared) {
    // The first segment is always allocated, and is at the end of the linked list
    struct region* region = (struct region*)shared;
    return region->segment_head->ro_words;
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
**/
size_t tm_size(shared_t shared) {
    struct region* region = (struct region*)shared;
    return region->segment_head->size * region->align;
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
**/
size_t tm_align(shared_t shared) {
    struct region* region = (struct region*)shared;
    return region->align;
}

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
**/
tx_t tm_begin(shared_t shared, bool is_ro) {
    struct region* region = (struct region*)shared;

    // Create a new transaction
    struct transaction* tx = malloc(sizeof(struct transaction));
    if (!tx) {
        return invalid_tx;
    }

    // Assign transaction properties
    tx->is_ro = is_ro;

    // Enter the batcher
    batcher_data* batcher = region->batcher;
    pthread_mutex_lock(&(batcher->batcher_mutex));
    while (batcher->wake_up_threads_only) {
        pthread_cond_broadcast(&(batcher->batcher_cond_enter));
    }
    if (batcher->remaining == 0) {
        // First thread entering, no one is blocked
        //printf("%ld: First thread entering\n", pthread_self());
        batcher->remaining = 1;
    } else {
        //printf("%ld: Blocked at %ld\n", pthread_self(), time(NULL));
        // Block this thread
        blocked_thread_node_t* new_node = (blocked_thread_node_t*)malloc(sizeof(blocked_thread_node_t));
        if (!new_node) {
            free(tx);
            pthread_mutex_unlock(&(batcher->batcher_mutex));
            return invalid_tx;
        }

        new_node->thread = pthread_self();
        new_node->next = batcher->blocked_head;
        batcher->blocked_head = new_node;

        // Increment the count of blocked threads
        batcher->blocked_count++;

        // Wait until "woken up"
        while (!batcher->wake_up_threads_only) {
            pthread_cond_wait(&(batcher->batcher_cond_wake_up), &(batcher->batcher_mutex));
        }
        //printf("%ld: Unblocked at %ld\n", pthread_self(), time(NULL));

        // Remove the thread from the blocked list
        blocked_thread_node_t* current = batcher->blocked_head;
        blocked_thread_node_t* previous = NULL;
        while (current) {
            if (current->thread == pthread_self()) {
                //printf("%ld: Removing thread from the blocked list\n", pthread_self());
                if (previous) {
                    previous->next = current->next;
                } else {
                    batcher->blocked_head = current->next;
                }
                free(current);
                batcher->blocked_count--;
                batcher->remaining++;
                if (batcher->blocked_count == 0) {
                    //printf("%ld: No more blocked threads\n", pthread_self());
                    batcher->wake_up_threads_only = false;
                    pthread_cond_broadcast(&(batcher->batcher_cond_enter));
                }
                break;
            }
            previous = current;
            current = current->next;
        }
    }

    pthread_mutex_unlock(&(batcher->batcher_mutex));
    //printf("%ld: Release the mutex at %ld\n", pthread_self(), time(NULL));

    return (tx_t)tx; // Ensure correct return type
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
**/
bool tm_end(shared_t shared, tx_t tx) {
    struct region* region = (struct region*)shared;
    struct transaction* transaction = (struct transaction*)tx;

    // Leave the batcher
    batcher_data* batcher = region->batcher;
    pthread_mutex_lock(&(batcher->batcher_mutex));
    batcher->remaining -= 1;
    if (batcher->remaining == 0) {
        //printf("%ld: Last thread leaving!\n", pthread_self());

        // End the epoch
        batcher->epoch += 1;
        batcher->wake_up_threads_only = true;

        //printf("%ld: New epoch: %ld\n", pthread_self(), batcher->epoch);


        // Update the segments (swap read-only and read-write segments, update control fields, etc.)
        dual_segment* current = region->segment_head;
        while (current) {
            // Swap the read-only and read-write segments
            uint8_t* temp = current->ro_words;
            current->ro_words = current->rw_words;
            current->rw_words = temp;

            // Reset the control fields
            for (size_t i = 0; i < current->size; i++) {
                current->controls[i].written_while_epoch = false;
                current->controls[i].access_set = ACC_SET_NONE;
            }

            current = current->next;
        }

        // Wake up all threads that are blocked
        pthread_cond_broadcast(&(batcher->batcher_cond_wake_up));
        //printf("%ld: Broadcasted wake up\n", pthread_self());
    }
    pthread_mutex_unlock(&(batcher->batcher_mutex));
    //printf("%ld: Release the mutex at %ld\n", pthread_self(), time(NULL));

    // Free the transaction
    free(transaction);

    return true;
}

/** [thread-safe] Read operation in the given transaction, source in the shared region and target in a private region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in the shared region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in a private region)
 * @return Whether the whole transaction can continue
**/
bool tm_read(shared_t shared, tx_t tx, void const* source, size_t size, void* target) {
    struct region* region = (struct region*)shared;
    struct transaction* transaction = (struct transaction*)tx;

    // Validate input parameters
    if (size == 0 || size % region->align != 0) {
        // Size must be a positive multiple of the alignment
        return false;
    }

    // Find the segment
    dual_segment* segment = find_segment(region, source);
    if (!segment) {
        // Source address not in any segment
        return false;
    }

    // Verify that we are not reading out of bounds
    size_t bytes_offset = (uint8_t*)source - segment->ro_words;
    if (bytes_offset + size > segment->size) {
        // Out of bounds
        return false;
    }

    // Calculate word offsets
    size_t word_size = region->align;                                    // Number of bytes per word
    size_t min_words_offset = bytes_offset / word_size;                  // Start word index
    size_t max_words_offset = (bytes_offset + size - 1) / word_size;     // End word index
    size_t num_words = max_words_offset - min_words_offset + 1;          // Total words to read

    // Array to keep track of acquired locks for unlocking later
    pthread_mutex_t* acquired_locks[num_words];

    // Lock the control fields for all words involved
    for (size_t i = min_words_offset, lock_index = 0; i <= max_words_offset; i++, lock_index++) {
        // Lock the control field mutex for each word
        pthread_mutex_t* mutex = &(segment->controls[i].word_mutex);
        pthread_mutex_lock(mutex);
        acquired_locks[lock_index] = mutex;  // Store the mutex to unlock later
    }


    // Check if the transaction can continue
    bool success = true;
    if (!transaction->is_ro) {
        for (size_t i = min_words_offset; i <= max_words_offset; i++) {
            controls* ctrl = &segment->controls[i];
            if (ctrl->written_while_epoch && ctrl->access_set != (intptr_t)transaction) {
                // Transaction must abort
                success = false;
                break;
            }
        }
    }

    // Abort if the transaction cannot continue
    if (!success) {
        // Unlock the control fields in reverse order to prevent potential deadlocks
        for (size_t i = num_words; i > 0; i--) {
            pthread_mutex_unlock(acquired_locks[i - 1]);
        }
        return false;
    }

    // Perform the read operation
    if (transaction->is_ro) {
        // Read-only transaction: read from the readable copy
        memcpy(target, segment->ro_words + bytes_offset, size);
    } else {
        // Read-write transaction: handle each word individually
        uint8_t* src_ptr = (uint8_t*)source;
        uint8_t* dst_ptr = (uint8_t*)target;
        size_t remaining_size = size;
        size_t current_word_offset = min_words_offset;

        while (remaining_size > 0) {
            controls* ctrl = &segment->controls[current_word_offset];

            if (ctrl->written_while_epoch) {
                if (ctrl->access_set == (intptr_t)transaction) {
                    // Transaction is already in the access set: read from writable copy
                    memcpy(dst_ptr, segment->rw_words + current_word_offset * word_size, word_size);
                } else {
                    // Should never happen, as we checked this before => aborted
                }
            } else {
                // The word was not written in the current epoch
                // Read from the readable copy
                memcpy(dst_ptr, segment->ro_words + current_word_offset * word_size, word_size);
                // Add transaction to the access set
                if (ctrl->access_set == ACC_SET_NONE) {
                    ctrl->access_set = (intptr_t)transaction;
                } else if (ctrl->access_set != (intptr_t)transaction && ctrl->access_set != ACC_SET_MULTIPLE) {
                    ctrl->access_set = ACC_SET_MULTIPLE;
                } else {
                    // Case 1: If mutiple => no need to change as we would change to multiple
                    // Case 2: If one but us => no need to change as we are already in the set
                }
            }

            // Move to the next word
            remaining_size -= word_size;
            dst_ptr += word_size;
            src_ptr += word_size;
            current_word_offset++;
        }
    }

    // Unlock the control fields in reverse order to prevent potential deadlocks
    for (size_t i = num_words; i > 0; i--) {
        pthread_mutex_unlock(acquired_locks[i - 1]);
    }

    return success;  // Return whether the transaction can continue
}


/** [thread-safe] Write operation in the given transaction, source in a private region and target in the shared region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in a private region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in the shared region)
 * @return Whether the whole transaction can continue
**/
bool tm_write(shared_t shared, tx_t tx, void const* source, size_t size, void* target) {
    struct region* region = (struct region*)shared;
    struct transaction* transaction = (struct transaction*)tx;

    // Validate input parameters
    if (size == 0 || size % region->align != 0) {
        // Size must be a positive multiple of the alignment
        return false;
    }

    // Find the segment
    dual_segment* segment = find_segment(region, target);
    if (!segment) {
        // Target address not in any segment
        return false;
    }

    // Verify that we are not writing out of bounds
    size_t bytes_offset = (uint8_t*)target - segment->ro_words;
    if (bytes_offset + size > segment->size) {
        // Out of bounds
        return false;
    }

    // Calculate word offsets
    size_t word_size = region->align;                                    // Number of bytes per word
    size_t min_words_offset = bytes_offset / word_size;                  // Start word index
    size_t max_words_offset = (bytes_offset + size - 1) / word_size;     // End word index
    size_t num_words = max_words_offset - min_words_offset + 1;          // Total words to write

    // Array to keep track of acquired locks for unlocking later
    pthread_mutex_t* acquired_locks[num_words];

    // Lock the control fields for all words involved
    for (size_t i = min_words_offset, lock_index = 0; i <= max_words_offset; i++, lock_index++) {
        pthread_mutex_t* mutex = &(segment->controls[i].word_mutex);
        pthread_mutex_lock(mutex);
        acquired_locks[lock_index] = mutex;  // Store the mutex to unlock later
    }

    // Check if the transaction can continue
    bool success = true;
    for (size_t i = min_words_offset; i <= max_words_offset; i++) {
        controls* ctrl = &segment->controls[i];

        if (ctrl->written_while_epoch) {
            if (ctrl->access_set == (intptr_t)transaction) {
                // Transaction is already in the access set: proceed
                continue;
            } else {
                // Transaction must abort due to write-write conflict
                success = false;
                break;
            }
        } else {
            if (ctrl->access_set != ACC_SET_NONE && ctrl->access_set != (intptr_t)transaction) {
                // At least one other transaction is in the access set: abort
                success = false;
                break;
            }
        }
    }

    if (success) {
        // Perform the write operation in a single memcpy
        memcpy(segment->rw_words + bytes_offset, source, size);

        // Update control structures for all words involved
        for (size_t i = min_words_offset; i <= max_words_offset; i++) {
            controls* ctrl = &segment->controls[i];

            // Add the transaction into the access set (if not already in)
            if (ctrl->access_set == ACC_SET_NONE) {
                ctrl->access_set = (intptr_t)transaction;
            } else if (ctrl->access_set != (intptr_t)transaction) {
                // Should not reach here since we checked above
            }

            // Mark that the word has been written in the current epoch
            ctrl->written_while_epoch = true;
        }
    }

    // Unlock the control fields in reverse order to prevent potential deadlocks
    for (size_t i = num_words; i > 0; i--) {
        pthread_mutex_unlock(acquired_locks[i - 1]);
    }

    return success;  // Return whether the transaction can continue
}

/** [thread-safe] Memory allocation in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param size   Allocation requested size (in bytes), must be a positive multiple of the alignment
 * @param target Pointer in private memory receiving the address of the first byte of the newly allocated, aligned segment
 * @return Whether the whole transaction can continue (success/nomem), or not (abort_alloc)
**/
alloc_t tm_alloc(shared_t shared, unused(tx_t tx), size_t size, void** target) {
    struct region* region = (struct region*)shared;

    // Validate input parameters
    if (size == 0 || size % region->align != 0) {
        // The length 'size' must be a positive multiple of the alignment
        // Behavior is undefined; returning abort_alloc as per guidelines
        return abort_alloc;
    }

    // Check that size is at most 2^48
    if (size > ((size_t)1 << 48)) {
        // Size exceeds maximum allowed value
        return nomem_alloc;
    }

    // Allocate the dual_segment structure
    dual_segment* segment = (dual_segment*)malloc(sizeof(dual_segment));
    if (!segment) {
        return nomem_alloc;
    }

    // Calculate the number of words
    size_t word_size = region->align;
    size_t num_words = size / word_size;
    segment->size = num_words;  // Number of words in the segment

    // Set can_be_freed to true (new segments can be freed)
    segment->can_be_freed = true;

    // Initialize next pointer
    segment->next = NULL;

    // Allocate memory for ro_words
    segment->ro_words = (uint8_t*)malloc(size);
    if (!segment->ro_words) {
        free(segment);
        return nomem_alloc;
    }

    // Allocate memory for rw_words
    segment->rw_words = (uint8_t*)malloc(size);
    if (!segment->rw_words) {
        free(segment->ro_words);
        free(segment);
        return nomem_alloc;
    }

    // Allocate memory for controls array
    segment->controls = (controls*)malloc(num_words * sizeof(controls));
    if (!segment->controls) {
        free(segment->rw_words);
        free(segment->ro_words);
        free(segment);
        return nomem_alloc;
    }

    // Initialize the copies to zeroes
    memset(segment->ro_words, 0, size);
    memset(segment->rw_words, 0, size);

    // Initialize the control structures
    for (size_t i = 0; i < num_words; i++) {
        // Initialize the mutex
        pthread_mutex_init(&segment->controls[i].word_mutex, NULL);

        // Initialize other fields
        segment->controls[i].written_while_epoch = false;
        segment->controls[i].access_set = ACC_SET_NONE;
    }

    // Register the segment in the set of allocated segments
    pthread_mutex_lock(&region->segments_mutex);

    // Add the segment to the head of the linked list
    segment->next = region->segment_head;
    region->segment_head = segment;

    pthread_mutex_unlock(&region->segments_mutex);

    // Set *target to the starting address of the segment's read-only words
    *target = (void*)segment->ro_words;

    // Ensure that *target is not NULL
    if (*target == NULL) {
        // This should not happen, but handle it just in case
        // Clean up resources
        for (size_t i = 0; i < num_words; i++) {
            pthread_mutex_destroy(&segment->controls[i].word_mutex);
        }
        free(segment->controls);
        free(segment->rw_words);
        free(segment->ro_words);
        free(segment);
        return nomem_alloc;
    }

    return success_alloc;  // Allocation was successful
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t shared, tx_t tx, void* target) {
    struct region* region = (struct region*)shared;

    // Find the segment to free
    dual_segment* current = region->segment_head;
    dual_segment* previous = NULL;
    while (current) {
        if (current->ro_words == target) {
            // Found the segment to free
            break;
        }
        previous = current;
        current = current->next;
    }

    if (!current) {
        // Segment not found
        return false;
    }

    // Check if the segment can be freed
    if (!current->can_be_freed) {
        // The segment cannot be freed
        return false;
    }

    // Lock the segment list
    pthread_mutex_lock(&region->segments_mutex);

    // Remove the segment from the list
    if (previous) {
        previous->next = current->next;
    } else {
        region->segment_head = current->next;
    }

    pthread_mutex_unlock(&region->segments_mutex);

    // Free the segment
    for (size_t i = 0; i < current->size; i++) {
        pthread_mutex_destroy(&current->controls[i].word_mutex);
    }
    free(current->controls);
    free(current->rw_words);
    free(current->ro_words);
    free(current);

    return true;
}

// -------------------------------------------- HELPER FUNCTIONS --------------------------------------------

dual_segment* find_segment(struct region* region, const void* addr) {
    dual_segment* current = region->segment_head;
    while (current) {
        void* start = current->ro_words;
        void* end = start + current->size * region->align;
        if (addr >= start && addr < end) {
            return current;
        }
        current = current->next;
    }
    return NULL;
}