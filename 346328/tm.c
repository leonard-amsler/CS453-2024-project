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

// Requested feature: pthread_rwlock_t
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#ifndef __USE_XOPEN2K
#define __USE_XOPEN2K
#endif

// External headers
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>
#include <pthread.h>
#include <tm.h>
#include <math.h>
#include <sys/time.h>
#include "macros.h"

// Constants for the access set
static const long ACC_SET_NONE = -1;
static const long ACC_SET_MULTIPLE = -2;

typedef struct running_thread_node {
    pthread_t thread;                       // Thread identifier
    struct running_thread_node* next;       // Next node in the list
} running_thread_node_t;

// Batcher data with synchronization primitives
typedef struct blocked_thread_node {
    pthread_t thread;                       // Thread identifier
    struct blocked_thread_node* next;       // Next node in the list
} blocked_thread_node_t;

// Batcher data with synchronization primitives
typedef struct {
    size_t epoch;                           // Current epoch
    size_t remaining;                       // Remaining transactions in the current epoch
    running_thread_node_t* running_head;    // Head of the running threads list
    blocked_thread_node_t* blocked_head;    // Head of the blocked threads list
    size_t blocked_count;                   // Number of blocked threads
    pthread_mutex_t batcher_mutex;          // Mutex to protect batcher data
    pthread_cond_t batcher_cond_wake_up;    // Condition variable to wake up threads that should be unblocked
    pthread_cond_t batcher_cond_enter;      // Condition variable to wake up threads that should enter
    int current_tx_id;                      // Current transaction identifier
    bool wake_up_threads_only;              // Whether to wake up threads only
} batcher_data;

// Control structure for each word
typedef struct {
    bool written_while_epoch;               // Whether the word was written in the current epoch
    void* owner;                            // Pointer to the transaction that owns this word
    pthread_rwlock_t word_mutex;             // Mutex to protect the word
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
struct undo_log_entry {
    void* addr;                             // Address in rw_words
    uint8_t* original_value;                // Original data
    size_t size;                            // Size of data
    struct undo_log_entry* next;            // Next entry in the undo log
};

struct transaction {
    bool is_ro;                             // Whether the transaction is read-only
    size_t id;                              // Transaction identifier
    struct undo_log_entry* undo_log_head;   // Head of the undo log
};


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

void add_to_undo_log(struct transaction* tx, void* addr, uint8_t* original_value, size_t size) {
    struct undo_log_entry* entry = malloc(sizeof(struct undo_log_entry));
    if (unlikely(!entry)) {
        return;
    }
    entry->addr = addr;
    entry->original_value = original_value;
    entry->size = size;
    entry->next = tx->undo_log_head;
    tx->undo_log_head = entry;
}

void rollback_transaction(struct region* region, struct transaction* tx) {
    struct undo_log_entry* log_entry = tx->undo_log_head;
    while (log_entry != NULL) {
        memcpy(log_entry->addr, log_entry->original_value, log_entry->size);
        struct undo_log_entry* temp = log_entry;
        log_entry = log_entry->next;
        free(temp->original_value);
        free(temp);
    }
    tx->undo_log_head = NULL;
}


// -------------------------------------------- DEBUGGING FUNCTIONS --------------------------------------------
// Function to print a given number of bytes from an array (for display purposes)
void print_bytes(uint8_t* data, size_t size) {
    // Print the combined value of the bytes
    // If word = [ 128 0 0 0 0 0 0 0 ] => combined = 128 (encoded order is big-endian)
    uint64_t combined = 0;
    for (size_t i = size; i > 0; i--) {
        combined = (combined << 8) | data[i - 1];
    }

    // Print that value
    printf("[ %zu ]", combined);

}

// Function to print a single controls structure as a row in a table
void print_controls_row(const controls* ctrl, size_t index) {
    printf("        | %-6zu | %-18s | %-18ld | %-23p |\n", index, ctrl->written_while_epoch ? "true" : "false", ctrl->owner, (void*)&ctrl->word_mutex);
}

// Function to print a dual_segment structure as a table
void print_dual_segment(const dual_segment* segment, const region* region) {



    printf("\n        Size (number of words): %zu\n", segment->size);
    printf("        Can Be Freed: %s\n", segment->can_be_freed ? "true" : "false");
    printf("        Next Segment: %p", (void*)segment->next);

    printf("\n\n        Read-Only Words (%p): ", (void*)segment->ro_words);
    size_t word_size = region->align;
    for (size_t i = 0; i < segment->size; i++) {
        uint8_t* word = segment->ro_words + i * word_size;
        print_bytes(word, word_size);
        printf(" ");
    }

    printf("\n\n        Read-Write Words (%p): ", (void*)segment->rw_words);
    for (size_t i = 0; i < segment->size; i++) {
        uint8_t* word = segment->rw_words + i * word_size;
        print_bytes(word, word_size);
        printf(" ");
    }

    printf("\n        Controls Table:\n");
    printf("        +--------+--------------------+--------------------+-------------------------+\n");
    printf("        | Index  | Written While Epoch|    Access Set      | Word Mutex Address      |\n");
    printf("        +--------+--------------------+--------------------+-------------------------+\n");
    for (size_t i = 0; i < segment->size; i++) {
        print_controls_row(&segment->controls[i], i);
    }
    printf("        +--------+--------------------+--------------------+-------------------------+\n");
}

// Function to print a region structure as a table
void print_region(const region* reg) {
    printf("\nRegion:\n");
    printf("\n  Alignment (bytes per word): %zu\n", reg->align);
    printf("  Segments Mutex Address: %p\n", (void*)&reg->segments_mutex);
    if (reg->batcher) {
        printf("  Batcher Address: %p\n", (void*)reg->batcher);
    } else {
        printf("  Batcher: NULL\n");
    }

    printf("\n  Segments List:\n");
    const dual_segment* current_segment = reg->segment_head;
    size_t segment_index = 0;
    while (current_segment) {
        printf("\n      Segment #%zu:\n", segment_index);
        print_dual_segment(current_segment, reg);
        current_segment = current_segment->next;
        segment_index++;
    }
}

long long current_time_in_us() {
    struct timeval time_now;
    gettimeofday(&time_now, NULL);
    return time_now.tv_sec * 1000000LL + time_now.tv_usec;
}

// ------------------------------------- TRANSACTION MANAGER FUNCTIONS --------------------------------------

/** Create a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
**/
shared_t tm_create(size_t size, size_t align) {

    // Verifications on the size
    if (unlikely((size <= 0) || (size > pow(2, 48)))) {
        return invalid_shared;
    }

    // Verifications on the alignment
    if (unlikely((align & (align - 1)) != 0)) {
        return invalid_shared;
    }

    // Verifications on the size and alignment
    if (unlikely(size % align != 0)) {
        return invalid_shared;
    }

    // Allocate the shared memory region
    struct region* region = malloc(sizeof(struct region));
    if (unlikely(!region)) {
        return invalid_shared;
    }

    // Initialize the region properties
    region->segment_head = NULL;
    region->align = align;
    if (unlikely(pthread_mutex_init(&(region->segments_mutex), NULL) != 0)) {
        free(region);
        return invalid_shared;
    }

    // Allocate and initialize the first segment
    dual_segment* segment = malloc(sizeof(dual_segment));
    if (unlikely(!segment)) {
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
    if (unlikely(posix_memalign((void**)&(segment->ro_words), align, size) != 0)) {
        free(segment);
        pthread_mutex_destroy(&(region->segments_mutex));
        free(region);
        return invalid_shared;
    }

    // Allocate rw_words
    if (unlikely(posix_memalign((void**)&(segment->rw_words), align, size) != 0)) {
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
    if (unlikely(!segment->controls)) {
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
        segment->controls[i].owner = NULL;
        if (unlikely(pthread_rwlock_init(&(segment->controls[i].word_mutex), NULL) != 0)) {
            // Clean up resources allocated so far
            for (size_t j = 0; j < i; j++) {
                pthread_rwlock_destroy(&(segment->controls[j].word_mutex));
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
    if (unlikely(!region->batcher)) {
        // Clean up resources
        for (size_t i = 0; i < num_words; i++) {
            pthread_rwlock_destroy(&(segment->controls[i].word_mutex));
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
    region->batcher->running_head = NULL;

    if (unlikely(pthread_mutex_init(&(region->batcher->batcher_mutex), NULL) != 0)) {
        free(region->batcher);
        // Clean up resources
        for (size_t i = 0; i < num_words; i++) {
            pthread_rwlock_destroy(&(segment->controls[i].word_mutex));
        }
        free(segment->controls);
        free(segment->rw_words);
        free(segment->ro_words);
        free(segment);
        pthread_mutex_destroy(&(region->segments_mutex));
        free(region);
        return invalid_shared;
    }

    if (unlikely(pthread_cond_init(&(region->batcher->batcher_cond_enter), NULL) != 0)) {
        pthread_mutex_destroy(&(region->batcher->batcher_mutex));
        free(region->batcher);
        // Clean up resources
        for (size_t i = 0; i < num_words; i++) {
            pthread_rwlock_destroy(&(segment->controls[i].word_mutex));
        }
        free(segment->controls);
        free(segment->rw_words);
        free(segment->ro_words);
        free(segment);
        pthread_mutex_destroy(&(region->segments_mutex));
        free(region);
        return invalid_shared;
    }

    if (unlikely(pthread_cond_init(&(region->batcher->batcher_cond_wake_up), NULL) != 0)) {
        pthread_cond_destroy(&(region->batcher->batcher_cond_enter));
        pthread_mutex_destroy(&(region->batcher->batcher_mutex));
        free(region->batcher);
        // Clean up resources
        for (size_t i = 0; i < num_words; i++) {
            pthread_rwlock_destroy(&(segment->controls[i].word_mutex));
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
    ////printf("\nDestroying shared memory region %p...\n", shared);

    // Verification on the shared memory region
    if (!shared) {
        return;
    }

    struct region* region = (struct region*)shared;

    // Free all segments
    dual_segment* current = region->segment_head;
    while (current) {
        dual_segment* next = current->next;

        // Free the controls array
        for (size_t i = 0; i < current->size; i++) {
            pthread_rwlock_destroy(&(current->controls[i].word_mutex));
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
    if (unlikely(!tx)) {
        return invalid_tx;
    }

    // Assign transaction properties
    tx->is_ro = is_ro;
    tx->undo_log_head = NULL;
    tx->id = __sync_fetch_and_add(&(region->batcher->current_tx_id), 1);

    // Variables to determine whether to broadcast
    bool should_broadcast_enter = false;
    bool should_broadcast_wake_up = false;

    // Helper variables
    batcher_data* batcher = region->batcher;

    pthread_mutex_lock(&(batcher->batcher_mutex));

    while (batcher->wake_up_threads_only) {

        // If we should wake up thread but no thread is blocked, we can proceed
        if (batcher->blocked_count == 0 && batcher->wake_up_threads_only) {
            batcher->wake_up_threads_only = false;
            should_broadcast_enter = true;
            break;
        }

        pthread_cond_wait(&(batcher->batcher_cond_enter), &(batcher->batcher_mutex));
    }


    if (batcher->remaining == 0) {
        // First thread entering, no one is blocked
        batcher->remaining = 1;
    } else {
        // Print all running threads
        running_thread_node_t* current_ = batcher->running_head;
        while (current_) {
            current_ = current_->next;
        }
        
        blocked_thread_node_t* new_node = (blocked_thread_node_t*)malloc(sizeof(blocked_thread_node_t));
        if (unlikely(!new_node)) {
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

        // Remove the thread from the blocked list
        blocked_thread_node_t* current = batcher->blocked_head;
        blocked_thread_node_t* previous = NULL;

        while (current) {
            if (current->thread == pthread_self()) {
                if (previous) {
                    previous->next = current->next;
                } else {
                    batcher->blocked_head = current->next;
                }
                free(current);
                batcher->blocked_count--;
                batcher->remaining++;
                if (batcher->blocked_count == 0) {
                    batcher->wake_up_threads_only = false;
                    should_broadcast_enter = true;
                } else {
                    should_broadcast_wake_up = true;
                }
                break;
            }
            previous = current;
            current = current->next;
        }
    }

    // Adding the thread to the running list
    running_thread_node_t* new_node = (running_thread_node_t*)malloc(sizeof(running_thread_node_t));
    if (unlikely(!new_node)) {
        free(tx);
        pthread_mutex_unlock(&(batcher->batcher_mutex));
        return invalid_tx;
    }
    new_node->thread = pthread_self();
    new_node->next = batcher->running_head;
    batcher->running_head = new_node;

    pthread_mutex_unlock(&(batcher->batcher_mutex));

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
    batcher_data* batcher = region->batcher;

    pthread_mutex_lock(&(batcher->batcher_mutex));

    batcher->remaining -= 1;
    bool should_broadcast_enter = false;
    bool should_broadcast_wake_up = false;

    if (batcher->remaining == 0) {

        // End the epoch
        batcher->epoch += 1;

        if (batcher->blocked_count > 0) {
            batcher->wake_up_threads_only = true;
            should_broadcast_wake_up = true;
        } else {
            batcher->wake_up_threads_only = false;
            should_broadcast_enter = true;
        }

        // Update the segments (swap read-only and read-write segments, update control fields, etc.)
        dual_segment* current = region->segment_head;
        while (current) {
            // Copy content from rw_words to ro_words
            memcpy(current->ro_words, current->rw_words, current->size * region->align);

            // Reset the control fields
            for (size_t i = 0; i < current->size; i++) {
                current->controls[i].written_while_epoch = false;
                current->controls[i].owner = NULL;
            }

            current = current->next;
        }

    }

    // Remove the thread from the running list
    running_thread_node_t* current = batcher->running_head;
    running_thread_node_t* previous = NULL;
    while (current) {
        if (current->thread == pthread_self()) {
            if (previous) {
                previous->next = current->next;
            } else {
                batcher->running_head = current->next;
            }
            free(current);
            break;
        }
        previous = current;
        current = current->next;
    }

    pthread_mutex_unlock(&(batcher->batcher_mutex));

    if (should_broadcast_enter) {
        pthread_cond_broadcast(&(batcher->batcher_cond_enter));
    } else if (should_broadcast_wake_up) {
        pthread_cond_broadcast(&(batcher->batcher_cond_wake_up));
    }

    // Free the undo log
    struct undo_log_entry* log_entry = transaction->undo_log_head;
    while (log_entry != NULL) {
        struct undo_log_entry* temp = log_entry;
        log_entry = log_entry->next;
        free(temp->original_value);
        free(temp);
    }
    transaction->undo_log_head = NULL;

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
    if (unlikely(size == 0 || size % region->align != 0)) {
        return false;
    }

    // Find the segment
    dual_segment* segment = find_segment(region, source);
    if (unlikely(!segment)) {
        return false;
    }

    // Verify that we are not reading out of bounds
    size_t bytes_offset = (uint8_t*)source - segment->ro_words;
    size_t segment_size_bytes = segment->size * region->align;
    if (unlikely(bytes_offset + size > segment_size_bytes)) {    
        return false;
    }

    size_t word_size = region->align;
    size_t min_word_index = bytes_offset / word_size;
    size_t max_word_index = (bytes_offset + size - 1) / word_size;

    bool success = true;

    // Read-only transactions can read from ro_words without locking
    if (transaction->is_ro) {
        memcpy(target, segment->ro_words + bytes_offset, size);
    } else {
        // Read-write transactions need to lock all words for reading
        size_t num_words = max_word_index - min_word_index + 1;
        pthread_rwlock_t** acquired_locks = malloc(num_words * sizeof(pthread_rwlock_t*));
        if (!acquired_locks) {
            return false;
        }

        // Acquire read locks in order to prevent deadlocks
        for (size_t i = min_word_index, lock_index = 0; i <= max_word_index; i++, lock_index++) {
            pthread_rwlock_t* rwlock = &(segment->controls[i].word_mutex);
            pthread_rwlock_rdlock(rwlock);
            acquired_locks[lock_index] = rwlock;

            // Check for conflicts
            controls* ctrl = &segment->controls[i];
            if (ctrl->written_while_epoch && ctrl->owner != transaction) {
                success = false;
                break;
            }
        }

        if (success) {
            // Read from rw_words if the word was written in this epoch, otherwise from ro_words
            uint8_t* dst_ptr = (uint8_t*)target;
            for (size_t i = min_word_index; i <= max_word_index; ++i) {
                controls* ctrl = &segment->controls[i];
                uint8_t* src_ptr = (ctrl->written_while_epoch && ctrl->owner == transaction)
                    ? (segment->rw_words + i * word_size)
                    : (segment->ro_words + i * word_size);

                memcpy(dst_ptr, src_ptr, word_size);
                dst_ptr += word_size;
            }
        }

        // Release locks
        for (size_t i = 0; i < (max_word_index - min_word_index + 1); i++) {
            pthread_rwlock_unlock(acquired_locks[i]);
        }
        free(acquired_locks);
    }

    if (!success) {
        return false;
    }
    
    return success;
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
    if (unlikely(size == 0 || size % region->align != 0)) {
        return false;
    }

    // Find the segment
    dual_segment* segment = find_segment(region, target);
    if (unlikely(!segment)) {
        return false;
    }

    // Verify that we are not writing out of bounds
    size_t bytes_offset = (uint8_t*)target - segment->ro_words;
    if (unlikely(bytes_offset + size > segment->size * region->align)) {
        return false;
    }

    size_t word_size = region->align;
    size_t min_word_index = bytes_offset / word_size;
    size_t max_word_index = (bytes_offset + size - 1) / word_size;
    size_t num_words = max_word_index - min_word_index + 1;

    // Acquire write locks for all words involved
    pthread_rwlock_t** acquired_locks = malloc(num_words * sizeof(pthread_rwlock_t*));
    if (!acquired_locks) {
        return false;
    }

    // For conflict detection
    bool conflict_detected = false;

    // Acquire write locks in order to prevent deadlocks
    for (size_t i = min_word_index, lock_index = 0; i <= max_word_index; i++, lock_index++) {
        pthread_rwlock_t* rwlock = &(segment->controls[i].word_mutex);
        pthread_rwlock_wrlock(rwlock);

        acquired_locks[lock_index] = rwlock;

        // Check for conflicts
        controls* ctrl = &segment->controls[i];
        if (ctrl->owner != NULL && ctrl->owner != transaction) {

            // Detail about the current tx (undo log)
            struct undo_log_entry* log_entry = transaction->undo_log_head;
            while (log_entry != NULL) {
                log_entry = log_entry->next;
            }

            // Detail about the owner tx (undo log)
            log_entry = ((struct transaction*)ctrl->owner)->undo_log_head;
            while (log_entry != NULL) {
                log_entry = log_entry->next;
            }

            conflict_detected = true;
            break;
        }
    }

    if (conflict_detected) {
        // Release locks
        for (size_t i = 0; i < num_words; i++) {
            pthread_rwlock_unlock(acquired_locks[i]);
        }
        free(acquired_locks);

        // Rollback changes
        struct undo_log_entry* log_entry = transaction->undo_log_head;
        while (log_entry != NULL) {
            memcpy(log_entry->addr, log_entry->original_value, log_entry->size);
            log_entry = log_entry->next;
        }

        // Free the undo log
        log_entry = transaction->undo_log_head;
        while (log_entry != NULL) {
            struct undo_log_entry* temp = log_entry;
            log_entry = log_entry->next;
            free(temp->original_value);
            free(temp);
        }
        transaction->undo_log_head = NULL;

        return false;
    }

    // Perform the write operation and update control structures
    uint8_t* src_ptr = (uint8_t*)source;
    for (size_t i = min_word_index; i <= max_word_index; i++) {
        size_t offset = i * word_size;
        controls* ctrl = &segment->controls[i];

        // Check if this address is already in the undo log
        bool already_logged = false;
        struct undo_log_entry* log_entry = transaction->undo_log_head;
        while (log_entry != NULL) {
            if (log_entry->addr == (segment->rw_words + offset)) {
                already_logged = true;
                break;
            }
            log_entry = log_entry->next;
        }

        if (!already_logged) {
            // Save the original value
            struct undo_log_entry* new_entry = malloc(sizeof(struct undo_log_entry));

            new_entry->addr = segment->rw_words + offset;
            new_entry->size = word_size;
            new_entry->original_value = malloc(word_size);
            memcpy(new_entry->original_value, new_entry->addr, word_size);
            new_entry->next = transaction->undo_log_head;
            transaction->undo_log_head = new_entry;
        }

        // Perform the write
        memcpy(segment->rw_words + offset, src_ptr, word_size);
        src_ptr += word_size;

        // Update control structures
        ctrl->owner = transaction;
        ctrl->written_while_epoch = true;
    }

    // Release locks
    for (size_t i = 0; i < num_words; i++) {
        pthread_rwlock_unlock(acquired_locks[i]);
    }
    free(acquired_locks);

    return true;
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
    if (unlikely(size == 0 || size % region->align != 0)) {
        // The length 'size' must be a positive multiple of the alignment
        // Behavior is undefined; returning abort_alloc as per guidelines
        return abort_alloc;
    }

    // Check that size is at most 2^48
    if (unlikely(size > ((size_t)1 << 48))) {
        // Size exceeds maximum allowed value
        return nomem_alloc;
    }

    // Allocate the dual_segment structure
    dual_segment* segment = (dual_segment*)malloc(sizeof(dual_segment));
    if (unlikely(!segment)) {
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
    if (unlikely(!segment->ro_words)) {
        free(segment);
        return nomem_alloc;
    }

    // Allocate memory for rw_words
    segment->rw_words = (uint8_t*)malloc(size);
    if (unlikely(!segment->rw_words)) {
        free(segment->ro_words);
        free(segment);
        return nomem_alloc;
    }

    // Allocate memory for controls array
    segment->controls = (controls*)malloc(num_words * sizeof(controls));
    if (unlikely(!segment->controls)) {
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
        segment->controls[i].owner = NULL;
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
    if (unlikely(*target == NULL)) {
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

