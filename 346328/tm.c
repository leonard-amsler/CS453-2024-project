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
#include <stdatomic.h>

/**
 * @brief Structure to store the batcher data
 */
typedef struct
{
    int epoch;                           // Current epoch
    int remaining;                       // Remaining transactions in the current epoch
    int blocked_count;                   // Number of blocked threads
    atomic_int current_tx_id;            // Current transaction identifier
    pthread_mutex_t batcher_mutex;       // Mutex to protect batcher data
    pthread_cond_t batcher_cond_wake_up; // Condition variable to wake up threads that should be unblocked
    pthread_cond_t batcher_cond_enter;   // Condition variable to wake up threads that should enter
    bool wake_up_threads_only;           // Whether to wake up threads only
} batcher_data;

/**
 * @brief Structure to store the control data for each word
 */
typedef struct
{
    bool written_while_epoch;    // Whether the word was written in the current epoch
    void *owner;                 // Pointer to the transaction that owns this word
    pthread_rwlock_t word_mutex; // Mutex to protect the word
} controls;

/**
 * @brief Structure to store a segment of the shared memory region
 */
typedef struct dual_segment
{
    uint8_t *ro_words;         // Read-only copy (array of bytes)
    uint8_t *rw_words;         // Writable copy (array of bytes)
    controls *controls;        // Control structures per word (array of controls)
    size_t size;               // Number of words in a segment
    bool can_be_freed;         // Whether the segment can be freed (Only the first segment cannot be freed)
    struct dual_segment *next; // Pointer to the next segment
} dual_segment;

/**
 * @brief Structure to store the shared memory region
 */
typedef struct region
{
    dual_segment *segment_head;     // Pointer to the first segment
    size_t align;                   // Number of bytes per word
    batcher_data *batcher;          // Batcher data for epoch management
    pthread_mutex_t segments_mutex; // Mutex to protect the segments list
} region;

/**
 * @brief Structure to store an entry in the undo log
 */
struct undo_log_entry
{
    void *addr;                  // Address in rw_words
    uint8_t *original_value;     // Original data (align bytes)
    struct undo_log_entry *next; // Next entry in the undo log
};

/**
 * @brief Structure to store a transaction
 */
struct transaction
{
    bool is_ro;                           // Whether the transaction is read-only
    size_t id;                            // Transaction identifier
    struct undo_log_entry *undo_log_head; // Head of the undo log
};

// -------------------------------------------- HELPER FUNCTIONS --------------------------------------------

/**
 * Function to find the segment that contains a given address
 * @param region The shared memory region
 * @param addr The address to find
 * @return The segment that contains the address, or NULL if the address is not in any segment
 */
dual_segment *find_segment(struct region *region, const void *addr)
{
    dual_segment *current_segment = region->segment_head;
    while (current_segment)
    {
        if ((addr >= current_segment->ro_words) && (addr < current_segment->ro_words + current_segment->size * region->align))
        {
            return current_segment;
        }
        current_segment = current_segment->next;
    }

    return NULL;
}

/**
 * Function to rollback the transaction by restoring the original values of the written words
 * @param region The shared memory region
 * @param tx The transaction to rollback
 */
void rollback_tx(struct region *region, struct transaction *tx)
{
    if (!tx->is_ro)
    {
        struct undo_log_entry *log_entry = tx->undo_log_head;
        while (log_entry != NULL)
        {
            uint8_t *addr = log_entry->addr;
            memcpy(addr, log_entry->original_value, region->align);
            log_entry = log_entry->next;
        }
    }

    tm_end((shared_t)region, (tx_t)tx);
}

// ------------------------------------- STM FUNCTIONS -------------------------------------

/**
 * Create a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
 **/
shared_t tm_create(size_t size, size_t align)
{
    // Verifications on the size
    if (unlikely((size <= 0) || (size > pow(2, 48))))
    {
        return invalid_shared;
    }

    // Verifications on the alignment
    if (unlikely((align & (align - 1)) != 0))
    {
        return invalid_shared;
    }

    // Verifications on the size and alignment
    if (unlikely(size % align != 0))
    {
        return invalid_shared;
    }

    // Allocate the shared memory region
    struct region *region = malloc(sizeof(struct region));
    if (unlikely(!region))
    {
        return invalid_shared;
    }

    // Initialize the region properties
    region->segment_head = NULL;
    region->align = align;
    if (unlikely(pthread_mutex_init(&(region->segments_mutex), NULL) != 0))
    {
        free(region);
        return invalid_shared;
    }

    // Allocate and initialize the first segment
    dual_segment *segment = malloc(sizeof(dual_segment));
    if (unlikely(!segment))
    {
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
    if (unlikely(posix_memalign((void **)&(segment->ro_words), align, size) != 0))
    {
        free(segment);
        pthread_mutex_destroy(&(region->segments_mutex));
        free(region);
        return invalid_shared;
    }

    // Allocate rw_words
    if (unlikely(posix_memalign((void **)&(segment->rw_words), align, size) != 0))
    {
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
    if (unlikely(!segment->controls))
    {
        free(segment->rw_words);
        free(segment->ro_words);
        free(segment);
        pthread_mutex_destroy(&(region->segments_mutex));
        free(region);
        return invalid_shared;
    }

    // Initialize the controls array
    for (size_t i = 0; i < num_words; i++)
    {
        segment->controls[i].written_while_epoch = false;
        segment->controls[i].owner = NULL;
        if (unlikely(pthread_rwlock_init(&(segment->controls[i].word_mutex), NULL) != 0))
        {
            for (size_t j = 0; j < i; j++)
            {
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
    if (unlikely(!region->batcher))
    {
        for (size_t i = 0; i < num_words; i++)
        {
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

    // Initialize the batcher data
    region->batcher->epoch = 0;
    region->batcher->remaining = 0;
    region->batcher->blocked_count = 0;
    region->batcher->wake_up_threads_only = false;
    atomic_init(&(region->batcher->current_tx_id), 0);

    // Initialize the batcher mutex
    if (unlikely(pthread_mutex_init(&(region->batcher->batcher_mutex), NULL) != 0))
    {
        free(region->batcher);
        for (size_t i = 0; i < num_words; i++)
        {
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

    // Initialize the batcher condition variables (enter)
    if (unlikely(pthread_cond_init(&(region->batcher->batcher_cond_enter), NULL) != 0))
    {
        pthread_mutex_destroy(&(region->batcher->batcher_mutex));
        free(region->batcher);
        for (size_t i = 0; i < num_words; i++)
        {
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

    // Initialize the batcher condition variables (wake up)
    if (unlikely(pthread_cond_init(&(region->batcher->batcher_cond_wake_up), NULL) != 0))
    {
        pthread_cond_destroy(&(region->batcher->batcher_cond_enter));
        pthread_mutex_destroy(&(region->batcher->batcher_mutex));
        free(region->batcher);
        for (size_t i = 0; i < num_words; i++)
        {
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
void tm_destroy(shared_t shared)
{
    if (!shared)
    {
        return;
    }

    struct region *region = (struct region *)shared;

    // Free all segments
    dual_segment *current = region->segment_head;
    while (current)
    {
        dual_segment *next = current->next;

        // Free the controls array
        for (size_t i = 0; i < current->size; i++)
        {
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
void *tm_start(shared_t shared)
{
    struct region *region = (struct region *)shared;
    return region->segment_head->ro_words;
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
 **/
size_t tm_size(shared_t shared)
{
    struct region *region = (struct region *)shared;
    return region->segment_head->size * region->align;
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
 **/
size_t tm_align(shared_t shared)
{
    struct region *region = (struct region *)shared;
    return region->align;
}

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
 **/
tx_t tm_begin(shared_t shared, bool is_ro)
{
    struct region *region = (struct region *)shared;

    // Create a new transaction
    struct transaction *tx = malloc(sizeof(struct transaction));
    if (unlikely(!tx))
    {
        return invalid_tx;
    }

    // Assign transaction properties
    tx->is_ro = is_ro;
    tx->undo_log_head = NULL;
    tx->id = atomic_fetch_add_explicit(&(region->batcher->current_tx_id), 1, memory_order_relaxed);

    // Variables to determine whether to broadcast
    bool should_broadcast_enter = false;
    bool should_broadcast_wake_up = false;

    // Lock the batcher mutex
    pthread_mutex_lock(&(region->batcher->batcher_mutex));

    // Wait for the signal to enter
    while (region->batcher->wake_up_threads_only && !is_ro)
    {
        if (region->batcher->blocked_count == 0 && region->batcher->wake_up_threads_only)
        {
            region->batcher->wake_up_threads_only = false;
            should_broadcast_enter = true;
            break;
        }
        pthread_cond_wait(&(region->batcher->batcher_cond_enter), &(region->batcher->batcher_mutex));
    }

    if (region->batcher->remaining == 0)
    {
        // We enter alone
        region->batcher->remaining = 1;
    }
    else
    {
        // Wait for the signal to wake up
        region->batcher->blocked_count++;
        while (!region->batcher->wake_up_threads_only)
        {
            pthread_cond_wait(&(region->batcher->batcher_cond_wake_up), &(region->batcher->batcher_mutex));
        }
        region->batcher->blocked_count--;

        // We wake up
        region->batcher->remaining++;
        if (region->batcher->blocked_count == 0)
        {
            // We are the last one to wake up
            region->batcher->wake_up_threads_only = false;
            should_broadcast_enter = true;
        }
        else
        {
            // We are not the last one to wake up, wake up the next one
            should_broadcast_wake_up = true;
        }
    }

    // Unlock the batcher mutex
    pthread_mutex_unlock(&(region->batcher->batcher_mutex));

    // Broadcast the condition variable
    if (should_broadcast_enter)
    {
        pthread_cond_broadcast(&(region->batcher->batcher_cond_enter));
    }
    else if (should_broadcast_wake_up)
    {
        pthread_cond_broadcast(&(region->batcher->batcher_cond_wake_up));
    }

    return (tx_t)tx; // Ensure correct return type
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
 **/
bool tm_end(shared_t shared, tx_t tx)
{
    // Helper variables
    struct region *region = (struct region *)shared;
    struct transaction *transaction = (struct transaction *)tx;

    // Lock the batcher mutex
    pthread_mutex_lock(&(region->batcher->batcher_mutex));

    // Decrement the remaining transactions
    region->batcher->remaining -= 1;
    bool should_broadcast_enter = false;
    bool should_broadcast_wake_up = false;

    if (region->batcher->remaining == 0)
    {
        //  We are the last transaction

        // Increment the epoch
        region->batcher->epoch += 1;

        // Wake up the blocked or enter threads
        if (region->batcher->blocked_count > 0)
        {
            region->batcher->wake_up_threads_only = true;
            should_broadcast_wake_up = true;
        }
        else
        {
            region->batcher->wake_up_threads_only = false;
            should_broadcast_enter = true;
        }

        // Set the write version to the read version, and reset the controls
        dual_segment *current = region->segment_head;
        while (current)
        {
            memcpy(current->ro_words, current->rw_words, current->size * region->align);

            for (size_t i = 0; i < current->size; i++)
            {
                current->controls[i].written_while_epoch = false;
                current->controls[i].owner = NULL;
            }

            current = current->next;
        }
    }

    // Unlock the batcher mutex
    pthread_mutex_unlock(&(region->batcher->batcher_mutex));

    // Broadcast the condition variable
    if (should_broadcast_enter)
    {
        pthread_cond_broadcast(&(region->batcher->batcher_cond_enter));
    }
    else if (should_broadcast_wake_up)
    {
        pthread_cond_broadcast(&(region->batcher->batcher_cond_wake_up));
    }

    // Free the logs of the transaction
    struct undo_log_entry *log_entry = transaction->undo_log_head;
    while (log_entry != NULL)
    {
        struct undo_log_entry *temp = log_entry;
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
bool tm_read(shared_t shared, tx_t tx, void const *source, size_t size, void *target)
{
    // Helper variables
    struct region *region = (struct region *)shared;
    struct transaction *transaction = (struct transaction *)tx;

    // Find the segment that contains the source address
    dual_segment *segment = find_segment(region, source);
    if (unlikely(!segment))
    {
        rollback_tx(region, transaction);
        return false;
    }

    // Calculate the offset of the source address in the segment
    size_t bytes_offset = (uint8_t *)source - segment->ro_words;
    size_t segment_size_bytes = segment->size * region->align;
    if (unlikely(bytes_offset + size > segment_size_bytes))
    {
        rollback_tx(region, transaction);
        return false;
    }

    // Calculate the word size and the word indexes
    size_t word_size = region->align;
    size_t min_word_index = bytes_offset / word_size;
    size_t max_word_index = (bytes_offset + size - 1) / word_size;

    bool success = true;

    // Read the data
    if (transaction->is_ro)
    {
        // Read the data in the read-only segment
        uint8_t *dst_ptr = (uint8_t *)target;
        for (size_t i = min_word_index; i <= max_word_index; ++i)
        {
            uint8_t *src_ptr = segment->ro_words + i * word_size;
            memcpy(dst_ptr, src_ptr, word_size);
            dst_ptr += word_size;
        }
    }
    else
    {
        // Read the data in the read-write segment

        // Acquire the locks
        size_t num_words = max_word_index - min_word_index + 1;
        pthread_rwlock_t **acquired_locks = malloc(num_words * sizeof(pthread_rwlock_t *));
        if (!acquired_locks)
        {
            rollback_tx(region, transaction);
            return false;
        }

        // Check for conflicts
        size_t lock_index = 0;
        for (size_t i = min_word_index; i <= max_word_index; i++)
        {
            pthread_rwlock_t *rwlock = &(segment->controls[i].word_mutex);
            pthread_rwlock_rdlock(rwlock);
            acquired_locks[lock_index] = rwlock;
            lock_index++;

            controls *ctrl = &segment->controls[i];
            if (ctrl->written_while_epoch && ctrl->owner != transaction)
            {
                success = false;
                break;
            }
        }

        if (success)
        {
            // If no conflict, read the data
            uint8_t *dst_ptr = (uint8_t *)target;
            for (size_t i = min_word_index; i <= max_word_index; ++i)
            {
                controls *ctrl = &segment->controls[i];
                // Choose the source pointer based on the ownership
                //      If the word was written in the current epoch by the current transaction, read from the rw_words
                //      Otherwise, read from the ro_words
                uint8_t *src_ptr = (ctrl->written_while_epoch && ctrl->owner == transaction)
                                       ? (segment->rw_words + i * word_size)
                                       : (segment->ro_words + i * word_size);

                memcpy(dst_ptr, src_ptr, word_size);
                dst_ptr += word_size;
            }
        }

        // Release the locks
        for (size_t i = 0; i < lock_index; i++)
        {
            pthread_rwlock_unlock(acquired_locks[i]);
        }
        free(acquired_locks);
    }

    // Rollback the transaction if a conflict was detected
    if (!success)
    {
        rollback_tx(region, transaction);
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
bool tm_write(shared_t shared, tx_t tx, void const *source, size_t size, void *target)
{
    // Helper variables
    struct region *region = (struct region *)shared;
    struct transaction *transaction = (struct transaction *)tx;

    // Find the segment that contains the target address
    dual_segment *segment = find_segment(region, target);
    if (unlikely(!segment))
    {
        rollback_tx(region, transaction);
        return false;
    }

    // Calculate the offset of the target address in the segment
    size_t bytes_offset = (uint8_t *)target - segment->ro_words;
    if (unlikely(bytes_offset + size > segment->size * region->align))
    {
        rollback_tx(region, transaction);
        return false;
    }

    // Calculate the word size and the word indexes
    size_t word_size = region->align;
    size_t min_word_index = bytes_offset / word_size;
    size_t max_word_index = (bytes_offset + size - 1) / word_size;
    size_t num_words = max_word_index - min_word_index + 1;

    // Acquire the locks
    pthread_rwlock_t **acquired_locks = malloc(num_words * sizeof(pthread_rwlock_t *));
    if (!acquired_locks)
    {
        rollback_tx(region, transaction);
        return false;
    }

    // Check for conflicts
    bool conflict_detected = false;
    size_t lock_index = 0;
    for (size_t i = min_word_index; i <= max_word_index; i++)
    {
        pthread_rwlock_t *rwlock = &(segment->controls[i].word_mutex);
        pthread_rwlock_wrlock(rwlock);
        acquired_locks[lock_index] = rwlock;
        lock_index++;

        controls *ctrl = &segment->controls[i];
        if (ctrl->owner != NULL && ctrl->owner != transaction)
        {
            conflict_detected = true;
            break;
        }
    }

    // Rollback the transaction if a conflict was detected (+ release the locks)
    if (conflict_detected)
    {
        for (size_t i = 0; i < lock_index; i++)
        {
            pthread_rwlock_unlock(acquired_locks[i]);
        }
        free(acquired_locks);

        rollback_tx(region, transaction);

        return false;
    }

    // Write the data in the rw_words and update the undo log in case of a rollback
    uint8_t *src_ptr = (uint8_t *)source;
    for (size_t i = min_word_index; i <= max_word_index; i++)
    {
        size_t offset = i * word_size;
        controls *ctrl = &segment->controls[i];

        memcpy(segment->rw_words + offset, src_ptr, word_size);
        src_ptr += word_size;

        ctrl->owner = transaction;
        ctrl->written_while_epoch = true;

        struct undo_log_entry *new_entry = malloc(sizeof(struct undo_log_entry));
        new_entry->addr = segment->rw_words + offset;
        new_entry->original_value = malloc(word_size);
        memcpy(new_entry->original_value, segment->ro_words + offset, word_size);
        new_entry->next = transaction->undo_log_head;
        transaction->undo_log_head = new_entry;
    }

    // Release the locks
    for (size_t i = 0; i < num_words; i++)
    {
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
alloc_t tm_alloc(shared_t shared, unused(tx_t tx), size_t size, void **target)
{
    // Helper variables
    struct region *region = (struct region *)shared;

    // Allocate a new segment
    dual_segment *segment = (dual_segment *)malloc(sizeof(dual_segment));
    if (unlikely(!segment))
    {
        return nomem_alloc;
    }

    // Initialize the segment
    segment->can_be_freed = true;
    segment->next = NULL;

    // Calculate the number of words
    size_t word_size = region->align;
    size_t num_words = size / word_size;
    segment->size = num_words;

    // Allocate the ro_words and rw_words
    if (unlikely(posix_memalign((void **)&(segment->ro_words), region->align, size) != 0))
    {
        free(segment);
        return nomem_alloc;
    }

    // Allocate the rw_words
    if (unlikely(posix_memalign((void **)&(segment->rw_words), region->align, size) != 0))
    {
        free(segment->ro_words);
        free(segment);
        return nomem_alloc;
    }

    // Initialize the ro_words and rw_words
    memset(segment->ro_words, 0, size);
    memset(segment->rw_words, 0, size);

    // Allocate the controls array
    segment->controls = malloc(num_words * sizeof(controls));
    if (unlikely(!segment->controls))
    {
        free(segment->rw_words);
        free(segment->ro_words);
        free(segment);
        return nomem_alloc;
    }

    // Initialize the controls array
    for (size_t i = 0; i < num_words; i++)
    {
        segment->controls[i].written_while_epoch = false;
        segment->controls[i].owner = NULL;
        if (unlikely(pthread_rwlock_init(&(segment->controls[i].word_mutex), NULL) != 0))
        {
            for (size_t j = 0; j < i; j++)
            {
                pthread_rwlock_destroy(&(segment->controls[j].word_mutex));
            }
            free(segment->controls);
            free(segment->rw_words);
            free(segment->ro_words);
            free(segment);
            rollback_tx(region, (struct transaction *)tx);
            return nomem_alloc;
        }
    }

    // Lock the segments mutex
    // pthread_mutex_lock(&(region->segments_mutex));

    if (region->segment_head == NULL)
    {
        // If the segment list is empty, set the new segment as the head
        region->segment_head = segment;
    }
    else
    {
        // Otherwise, add the new segment to the end of the list
        dual_segment *current = region->segment_head;
        while (current->next)
        {
            current = current->next;
        }
        current->next = segment;
    }

    // Unlock the segments mutex
    // pthread_mutex_unlock(&(region->segments_mutex));

    // Return the target address
    *target = segment->ro_words;

    return success_alloc;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
 **/
bool tm_free(shared_t shared, unused(tx_t tx), void *target)
{
    // Helper variables
    struct region *region = (struct region *)shared;

    // Find the segment that contains the target address
    dual_segment *segment = find_segment(region, target);
    if (unlikely(!segment))
    {
        rollback_tx(region, (struct transaction *)tx);
        return false;
    }

    // Check if the segment can be freed (first segment cannot be freed)
    if (!segment->can_be_freed)
    {
        rollback_tx(region, (struct transaction *)tx);
        return false;
    }

    // Lock the segments mutex
    pthread_mutex_lock(&(region->segments_mutex));

    // Remove the segment from the list
    dual_segment *current = region->segment_head;
    dual_segment *previous = NULL;
    while (current)
    {
        if (current == segment)
        {
            if (previous)
            {
                previous->next = current->next;
            }
            else
            {
                region->segment_head = current->next;
            }
            break;
        }
        previous = current;
        current = current->next;
    }

    // Unlock the segments mutex
    pthread_mutex_unlock(&(region->segments_mutex));

    // Free the segment
    for (size_t i = 0; i < segment->size; i++)
    {
        pthread_rwlock_destroy(&(segment->controls[i].word_mutex));
    }
    free(segment->controls);
    free(segment->rw_words);
    free(segment->ro_words);
    free(segment);

    return true;
}
