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

// Dual segment structure with redundant size field
typedef struct {
    bool written_while_epoch;               // Whether the segment was written during the current epoch
    long access_set;                        // Access set of the segment, if one => pointer of the transaction object, if none => -1, if multiple => -2
    bool can_be_freed;                      // Whether the segment can be freed (only the first segment cannot be freed)
    pthread_mutex_t segment_mutex;          // Mutex to protect the segment
} controls;

// Dual segment structure without redundant size field
typedef struct {
    uint8_t* ro_words;                      // Pointer to the next dual segment
    uint8_t* rw_words;                      // Pointer to the previous dual segment
    controls* controls;                     // Pointer to the control fields
    size_t size;                            // Number of words per segment
} dual_segment;

// Shared memory region structure
typedef struct region {
    dual_segment* dual_segments;             // Pointer to the first segment
    size_t segment_count;                   // Number of segments
    size_t align;                           // Number of bytes per word
    batcher_data* batcher;                  // Batcher data for epoch management
};

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

    // Verifications
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

    // Assign the region properties
    region->segment_count = 1;
    region->align = align;
    region->dual_segments = malloc(sizeof(dual_segment));
    if (!region->dual_segments) {
        printf("Memory allocation failed for the dual segment\n");
        free(region);
        return invalid_shared;
    }

    // Assign and initialize the first segment properties
    region->dual_segments->size = size;
    
    // Allocate read-only segment
    if (posix_memalign(&(region->dual_segments->ro_words), align, size) != 0) {
        printf("Memory allocation failed for the read-only segment\n");
        free(region->dual_segments);
        free(region);
        return invalid_shared;
    }
    
    // Allocate read-write segment
    if (posix_memalign(&(region->dual_segments->rw_words), align, size) != 0) {
        printf("Memory allocation failed for the read-write segment\n");
        free(region->dual_segments->ro_words); // Free the read-only segment
        free(region->dual_segments); // Free the dual segment
        free(region); // Free the region
        return invalid_shared;
    }

    // Initialize memory
    memset(region->dual_segments->ro_words, 0, size);
    memset(region->dual_segments->rw_words, 0, size);
    
    // Initialize segment properties
    region->dual_segments->controls = malloc(sizeof(controls));
    if (!region->dual_segments->controls) {
        printf("Memory allocation failed for the controls\n");
        free(region->dual_segments->ro_words); // Free the read-only segment
        free(region->dual_segments->rw_words); // Free the read-write segment
        free(region->dual_segments); // Free the dual segment
        free(region); // Free the region
        return invalid_shared;
    }

    // Initialize the control fields
    region->dual_segments->controls->written_while_epoch = false;
    region->dual_segments->controls->access_set = -1;
    region->dual_segments->controls->can_be_freed = false;
    if(pthread_mutex_init(&(region->dual_segments->controls->segment_mutex), NULL) != 0) {
        printf("Mutex initialization failed for the segment\n");
        free(region->dual_segments->controls); // Free the control fields
        free(region->dual_segments->ro_words); // Free the read-only segment
        free(region->dual_segments->rw_words); // Free the read-write segment
        free(region->dual_segments); // Free the dual segment
        free(region); // Free the region
        return invalid_shared;
    }

    // Assign and initialize the batcher data
    region->batcher = malloc(sizeof(batcher_data));
    if (!region->batcher) {
        printf("Memory allocation failed for the batcher\n");
        free(region->dual_segments->controls); // Free the control fields
        free(region->dual_segments->ro_words); // Free the read-only segment
        free(region->dual_segments->rw_words); // Free the read-write segment
        free(region->dual_segments); // Free the dual segment
        free(region); // Free the region
        return invalid_shared;
    }
    region->batcher->epoch = 0;
    region->batcher->remaining = 0;
    region->batcher->blocked_head = NULL;
    region->batcher->blocked_count = 0;
    region->batcher->wake_up_threads_only = false;
    if(pthread_mutex_init(&(region->batcher->batcher_mutex), NULL) != 0) {
        printf("Mutex initialization failed for the batcher\n");
        free(region->batcher); // Free the batcher
        free(region->dual_segments->controls); // Free the control fields
        free(region->dual_segments->ro_words); // Free the read-only segment
        free(region->dual_segments->rw_words); // Free the read-write segment
        free(region->dual_segments); // Free the dual segment
        free(region); // Free the region
        return invalid_shared;
    }

    if (pthread_cond_init(&(region->batcher->batcher_cond_enter), NULL) != 0) {
        printf("Condition variable initialization failed for the batcher\n");
        pthread_mutex_destroy(&(region->batcher->batcher_mutex));
        free(region->batcher); // Free the batcher
        free(region->dual_segments->controls); // Free the control fields
        free(region->dual_segments->ro_words); // Free the read-only segment
        free(region->dual_segments->rw_words); // Free the read-write segment
        free(region->dual_segments); // Free the dual segment
        free(region); // Free the region
        return invalid_shared;
    }
    
    if(pthread_cond_init(&(region->batcher->batcher_cond_wake_up), NULL) != 0) {
        printf("Condition variable initialization failed for the batcher\n");
        pthread_mutex_destroy(&(region->batcher->batcher_mutex));
        pthread_cond_destroy(&(region->batcher->batcher_cond_enter));
        free(region->batcher); // Free the batcher
        free(region->dual_segments->controls); // Free the control fields
        free(region->dual_segments->ro_words); // Free the read-only segment
        free(region->dual_segments->rw_words); // Free the read-write segment
        free(region->dual_segments); // Free the dual segment
        free(region); // Free the region
        return invalid_shared;
    }

    return (shared_t)region; // Ensure correct return type
}


/** Destroy a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t shared) {
    struct region* region = (struct region*)shared;

    // Free all segments
    for (size_t i = 0; i < region->segment_count; i++) {
        dual_segment* seg = &(region->dual_segments[i]);
        
        // Free individual segment properties if they were dynamically allocated
        free(seg->ro_words);
        free(seg->rw_words);
        free(seg->controls);
    }

    // Free the dual segment array
    free(region->dual_segments);

    // Free the batcher data
    pthread_mutex_destroy(&region->batcher->batcher_mutex);
    pthread_cond_destroy(&region->batcher->batcher_cond_wake_up);
    pthread_cond_destroy(&region->batcher->batcher_cond_enter);
    
    // Free the blocked threads list if dynamically allocated
    blocked_thread_node_t* current = region->batcher->blocked_head;
    while (current) {
        blocked_thread_node_t* next = current->next;
        free(current);
        current = next;
    }

    // Free the batcher structure itself
    free(region->batcher);

    // Free the region
    free(region);
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t shared) {
    // The first segment is always allocated, and is at the end of the linked list
    struct region* region = (struct region*)shared;
    return region->dual_segments->ro_words;

}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
**/
size_t tm_size(shared_t shared) {
    struct region* region = (struct region*)shared;
    return region->dual_segments->size;
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
        dual_segment* segments = region->dual_segments;
        for (size_t i = 0; i < region->segment_count; i++) {
            // No need for locks as we no that no other thread is accessing the segments
            dual_segment* current = &(segments[i]);
            void* temp = current->ro_words;
            current->ro_words = current->rw_words;
            current->rw_words = temp;
            current->controls->written_while_epoch = false;
            current->controls->access_set = -1;
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
    size_t word_size = region->align;
    size_t min_words_offset = bytes_offset / word_size;                  // Start word index
    size_t max_words_offset = (bytes_offset + size - 1) / word_size;     // End word index
    size_t num_words = max_words_offset - min_words_offset + 1;          // Total words to read

    // Array to keep track of acquired locks for unlocking later
    pthread_mutex_t* acquired_locks[num_words];

    // Lock the control fields for all words involved
    for (size_t i = min_words_offset, lock_index = 0; i <= max_words_offset; i++, lock_index++) {
        // Lock the control field mutex for each word
        pthread_mutex_t* mutex = &(segment->controls[i].segment_mutex);
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
                    // Transaction must abort
                    success = false;
                    break;
                }
            } else {
                // The word was not written in the current epoch
                // Read from the readable copy
                memcpy(dst_ptr, segment->ro_words + current_word_offset * word_size, word_size);
                // Add transaction to the access set
                if (ctrl->access_set == -1) {
                    ctrl->access_set = (intptr_t)transaction;
                } else if (ctrl->access_set != (intptr_t)transaction && ctrl->access_set != -2) {
                    ctrl->access_set = -2;
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
    return false;
}

/** [thread-safe] Memory allocation in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param size   Allocation requested size (in bytes), must be a positive multiple of the alignment
 * @param target Pointer in private memory receiving the address of the first byte of the newly allocated, aligned segment
 * @return Whether the whole transaction can continue (success/nomem), or not (abort_alloc)
**/
alloc_t tm_alloc(shared_t shared, tx_t tx, size_t size, void** target) {
    return abort_alloc;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t shared, tx_t tx, void* target) {
    return false;
}

// -------------------------------------------- HELPER FUNCTIONS --------------------------------------------

dual_segment* find_segment(struct region* region, const void* addr) {
    for (size_t i = 0; i < region->segment_count; i++) {
        dual_segment* segment = &region->dual_segments[i];
        uint8_t* start = segment->ro_words;
        uint8_t* end = start + segment->size;
        if (addr >= (void*)start && addr < (void*)end) {
            return segment;
        }
    }
    return NULL;
}