#include <stdio.h>
#include <unistd.h>  // For sleep
#include <pthread.h> // For pthread_create and pthread_join
#include <stdlib.h>  // For exit
#include <stdbool.h> // For bool type
#include "tm.h"      // Include your tm.h file

#define TEST_ASSERT(cond, msg) \
    if (!(cond)) { \
        fprintf(stderr, "Test failed: %s\n", msg); \
        exit(EXIT_FAILURE); \
    } else { \
        printf("Test passed: %s\n", msg); \
    }

#define NUM_THREADS 100

// Structure to hold thread data
typedef struct {
    int id;
    size_t size;
    size_t align;
    shared_t shared_region;
    void* start;
    tx_t tx;
    size_t read_size;
    size_t read_offset;
} thread_data_t;

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

// ------------------------------------- TM_CREATE -------------------------------------

void unit_tests_tm_create() {
    printf("\n----------------------- TM_CREATE ------------------------\n");
    unit_test_tm_create_invalid_size();
    unit_test_tm_create_invalid_alignment();
    unit_test_tm_create_valid_single();
    unit_test_tm_create_valid_concurrent();
}

void unit_test_tm_create_invalid_size() {
    printf("\nRunning unit test for tm_create with invalid size...\n");
    // Size must be positive, a multiple of the alignment and at most 2 ^ 48 (2 ^ 48 = 281474976710656 bytes = 256 TB)
    size_t align = 16;

    size_t size_neg = -1;
    shared_t shared_region_neg = tm_create(size_neg, align);
    TEST_ASSERT(shared_region_neg == invalid_shared, "tm_create fail with negative size");

    size_t size_zero = 0;
    shared_t shared_region_zero = tm_create(size_zero, align);
    TEST_ASSERT(shared_region_zero == invalid_shared, "tm_create fail with zero size");

    size_t size_one = 1;
    shared_t shared_region_one = tm_create(size_one, align);
    TEST_ASSERT(shared_region_one == invalid_shared, "tm_create fail with size one");

    size_t size_not_multiple = 15;
    shared_t shared_region_not_multiple = tm_create(size_not_multiple, align);
    TEST_ASSERT(shared_region_not_multiple == invalid_shared, "tm_create fail with size not multiple of alignment");

    size_t size_too_large = 2 << 48 + align;
    shared_t shared_region_too_large = tm_create(size_too_large, align);
    TEST_ASSERT(shared_region_too_large == invalid_shared, "tm_create fail with size too large");

    tm_destroy(shared_region_neg);
    tm_destroy(shared_region_zero);
    tm_destroy(shared_region_one);
    tm_destroy(shared_region_not_multiple);
    tm_destroy(shared_region_too_large);

}

void unit_test_tm_create_invalid_alignment() {
    printf("\nRunning unit test for tm_create with invalid alignment...\n");
    // Alignment must be a power of 2
    size_t size = 16;

    size_t align_not_power_of_two = 15;
    shared_t shared_region_not_power_of_two = tm_create(size, align_not_power_of_two);
    TEST_ASSERT(shared_region_not_power_of_two == invalid_shared, "tm_create fail with alignment not power of two");

    size_t align_neg = -1;
    shared_t shared_region_neg = tm_create(size, align_neg);
    TEST_ASSERT(shared_region_neg == invalid_shared, "tm_create fail with negative alignment");

    size_t align_zero = 0;
    shared_t shared_region_zero = tm_create(size, align_zero);
    TEST_ASSERT(shared_region_zero == invalid_shared, "tm_create fail with zero alignment");

    tm_destroy(shared_region_not_power_of_two);
    tm_destroy(shared_region_neg);
    tm_destroy(shared_region_zero);
}

void unit_test_tm_create_valid_single() {
    printf("\nRunning unit test for tm_create with valid parameters...\n");
    size_t size = 32;
    size_t align = 16;
    shared_t shared_region = tm_create(size, align);

    // Creation sucessfull
    TEST_ASSERT(shared_region != invalid_shared, "tm_create success with valid parameters");

    // The first allocated segment must be initialized with zeroes.
    void* start = tm_start(shared_region);
    size_t segment_size = tm_size(shared_region);
    bool success = true;
    for (size_t i = 0; i < segment_size; i++) {
        uint8_t* byte = (uint8_t*)start + i;
        if (*byte != 0) {
            success = false;
            break;
        }
    }
    TEST_ASSERT(success, "tm_create makes the first segment zeroed");

    // The first allocated segment cannot be freed with tm free.
    bool can_free = tm_free(shared_region, 0, start);
    TEST_ASSERT(!can_free, "tm_create makes the first segment not freeable");

    tm_destroy(shared_region);
}

void* thread_tm_create(void* arg) {
    thread_data_t* data = (thread_data_t*)arg;
    data->shared_region = tm_create(data->size, data->align);
    return NULL;
}

void unit_test_tm_create_valid_concurrent() {
    printf("\nRunning unit test for tm_create in concurrent manner...\n");

    pthread_t threads[NUM_THREADS];
    thread_data_t thread_data[NUM_THREADS];

    size_t size = 32;
    size_t align = 16;

    // Initialize thread data and create threads
    
    bool success = true;
    for (int i = 0; i < NUM_THREADS; i++) {
        thread_data[i].size = size;
        thread_data[i].align = align;
        thread_data[i].shared_region = invalid_shared;
        thread_data[i].id = i;

        int rc = pthread_create(&threads[i], NULL, thread_tm_create, (void*)&thread_data[i]);
        if (rc != 0) {
            success = false;
            break;  
        }
    }
    TEST_ASSERT(success, "pthread_create success multiple threads");

    // Wait for all threads to finish
    for (int i = 0; i < NUM_THREADS; i++) {
        int rc = pthread_join(threads[i], NULL);
        if (rc != 0) {
            success = false;
        }
    }
    TEST_ASSERT(success, "pthread_join success multiple threads");

    // Verify that each shared_region is valid and independent
    char msg[100] = "shared_region and start are for each thread and correctly initialized";
    for (int i = 0; i < NUM_THREADS; i++) {
        shared_t shared_region = thread_data[i].shared_region;
        if (shared_region == invalid_shared) {
            success = false;
            snprintf(msg, sizeof(msg), "tm_create returns unvalid shared region in thread %d", i);
            break;
        }

        // Verify that the starting address is not NULL
        void* start = tm_start(shared_region);
        if (start == NULL) {
            success = false;
            snprintf(msg, sizeof(msg), "tm_create returns NULL start address in thread %d", i);
            break;
        }

        // Verify that the memory is zero-initialized
        size_t segment_size = tm_size(shared_region);
        bool success = true;
        for (size_t j = 0; j < segment_size; j++) {
            uint8_t* byte = (uint8_t*)start + j;
            if (*byte != 0) {
                success = false;
                snprintf(msg, sizeof(msg), "tm_create initializes memory not to zero in thread %d", i);
                break;
            }
        }
    }
    TEST_ASSERT(success, msg);

    // Verify that the shared regions are independent
    snprintf(msg, sizeof(msg), "shared regions are independent for each thread");
    for (int i = 0; i < NUM_THREADS; i++) {
        for (int j = i + 1; j < NUM_THREADS; j++) {
            shared_t shared_region_i = thread_data[i].shared_region;
            shared_t shared_region_j = thread_data[j].shared_region;
            if (shared_region_i == shared_region_j) {
                success = false;
                snprintf(msg, sizeof(msg), "shared regions are not independent in threads %d and %d", i, j);
                break;
            }
        }
    }
    TEST_ASSERT(success, msg);

    // Clean up: Destroy all shared memory regions
    for (int i = 0; i < NUM_THREADS; i++) {
        shared_t shared_region = thread_data[i].shared_region;
        if (shared_region != invalid_shared) {
            tm_destroy(shared_region);
        }
    }
}

// ------------------------------------- TM_DESTROY -------------------------------------

void unit_tests_tm_destroy() {
    printf("\n----------------------- TM_DESTROY ------------------------\n");
    unit_test_tm_destroy_invalid_shared();
    unit_test_tm_destroy_valid_single();
    unit_test_tm_destroy_valid_concurrent();
}

void unit_test_tm_destroy_invalid_shared() {
    printf("\nRunning unit test for tm_destroy with invalid shared region...\n");
    shared_t invalid_shared = NULL;
    tm_destroy(invalid_shared);
    TEST_ASSERT(true, "tm_destroy success with invalid shared region");
}

void unit_test_tm_destroy_valid_single() {
    printf("\nRunning unit test for tm_destroy with valid shared region...\n");
    size_t size = 32;
    size_t align = 16;
    shared_t shared_region = tm_create(size, align);
    tm_destroy(shared_region);
    TEST_ASSERT(true, "tm_destroy success with valid shared region");
}

void* thread_tm_destroy(void* arg) {
    thread_data_t* data = (thread_data_t*)arg;
    tm_destroy(data->shared_region);
    return NULL;
}

void unit_test_tm_destroy_valid_concurrent() {
    printf("\nRunning unit test for tm_destroy in concurrent manner...\n");

    pthread_t threads[NUM_THREADS];
    thread_data_t thread_data[NUM_THREADS];

    size_t size = 32;
    size_t align = 16;

    // Initialize thread data and create threads
    bool success = true;
    for (int i = 0; i < NUM_THREADS; i++) {
        thread_data[i].size = size;
        thread_data[i].align = align;
        thread_data[i].shared_region = tm_create(size, align);
        thread_data[i].id = i;

        int rc = pthread_create(&threads[i], NULL, thread_tm_destroy, (void*)&thread_data[i]);
        if (rc != 0) {
            success = false;
        }
    }
    TEST_ASSERT(success, "pthread_create success multiple threads");

    // Wait for all threads to finish
    for (int i = 0; i < NUM_THREADS; i++) {
        int rc = pthread_join(threads[i], NULL);
        if (rc != 0) {
            success = false;
        }
    }
    TEST_ASSERT(success, "pthread_join success multiple threads");
}

// ------------------------------------- TM_START -------------------------------------

void unit_tests_tm_start() {
    printf("\n----------------------- TM_START ------------------------\n");
    unit_test_tm_start_valid_single();
    unit_test_tm_start_valid_concurrent();
}

void unit_test_tm_start_valid_single() {
    printf("\nRunning unit test for tm_start with valid shared region...\n");
    size_t size = 32;
    size_t align = 16;
    shared_t shared_region = tm_create(size, align);
    void* start = tm_start(shared_region);
    size_t segment_size = tm_size(shared_region);
    TEST_ASSERT(start != NULL, "tm_start success with valid shared region");
    TEST_ASSERT(segment_size == size, "tm_start returns correct start address");

    // The returned address must be aligned on the shared region alignment.
    uintptr_t start_addr = (uintptr_t)start;
    TEST_ASSERT(start_addr % align == 0, "tm_start returns aligned start address");

    // This function never fails: it must always return the address of the first allocated segment, which is not free-able.
    bool can_free = tm_free(shared_region, 0, start);
    TEST_ASSERT(!can_free, "tm_start makes the first segment not freeable");

    // The returned pointer must not be NULL (or nullptr in C++), and must not change between invocations.
    void* start2 = tm_start(shared_region);
    TEST_ASSERT(start2 == start, "tm_start returns the same start address");

    tm_destroy(shared_region);
}

void* thread_tm_start(void* arg) {
    thread_data_t* data = (thread_data_t*)arg;
    data->start = tm_start(data->shared_region);
    return NULL;
}

void unit_test_tm_start_valid_concurrent() {
    printf("\nRunning unit test for tm_start in concurrent manner...\n");

    pthread_t threads[NUM_THREADS];
    thread_data_t thread_data[NUM_THREADS];

    size_t size1 = 32;
    size_t align1 = 16;
    shared_t shared1 = tm_create(size1, align1);

    size_t size2 = 64;
    size_t align2 = 32;
    shared_t shared2 = tm_create(size2, align2);

    // Initialize thread data and create threads
    bool success = true;
    for (int i = 0; i < NUM_THREADS; i++) {
        thread_data[i].size = i % 2 == 0 ? size1 : size2;
        thread_data[i].align = i % 2 == 0 ? align1 : align2;
        thread_data[i].shared_region = i % 2 == 0 ? shared1 : shared2;
        thread_data[i].start = NULL;
        thread_data[i].id = i;

        int rc = pthread_create(&threads[i], NULL, thread_tm_start, (void*)&thread_data[i]);
        if (rc != 0) {
            success = false;
        }
    }
    TEST_ASSERT(success, "pthread_create success multiple threads");

    // Wait for all threads to finish
    for (int i = 0; i < NUM_THREADS; i++) {
        int rc = pthread_join(threads[i], NULL);
        if (rc != 0) {
            success = false;
        }
    }
    TEST_ASSERT(success, "pthread_join success multiple threads");

    // The returned address must be aligned on the shared region alignment.
    char msg[100] = "start address is aligned for each thread";
    for (int i = 0; i < NUM_THREADS; i++) {
        void* start = thread_data[i].start;
        uintptr_t start_addr = (uintptr_t)start;
        size_t align = thread_data[i].align;
        if (start_addr % align != 0) {
            success = false;
            snprintf(msg, sizeof(msg), "tm_start returns unaligned start address in thread %d", i);
            break;
        }
    }
    TEST_ASSERT(success, msg);

    // This function never fails: it must always return the address of the first allocated segment, which is not free-able.
    snprintf(msg, sizeof(msg), "start address is not freeable for each thread");
    for (int i = 0; i < NUM_THREADS; i++) {
        void* start = thread_data[i].start;
        bool can_free = tm_free(thread_data[i].shared_region, 0, start);
        if (can_free) {
            success = false;
            snprintf(msg, sizeof(msg), "tm_start returns freeable start address in thread %d", i);
            break;
        }
    }
    TEST_ASSERT(success, msg);

    // The returned pointer must not be NULL (or nullptr in C++), and must not change between invocations.
    snprintf(msg, sizeof(msg), "start address is the same for each thread");
    for (int i = 0; i < NUM_THREADS; i++) {
        void* start = thread_data[i].start;
        void* start2 = tm_start(thread_data[i].shared_region);
        if (start2 != start) {
            success = false;
            snprintf(msg, sizeof(msg), "tm_start returns different start address in thread %d", i);
            break;
        }
    }
    TEST_ASSERT(success, msg);

    // Verify that each start address is not NULL
    snprintf(msg, sizeof(msg), "start address is not NULL for each thread");
    for (int i = 0; i < NUM_THREADS; i++) {
        void* start = thread_data[i].start;
        if (start == NULL) {
            success = false;
            snprintf(msg, sizeof(msg), "tm_start returns NULL start address in thread %d", i);
            break;
        }
    }
    TEST_ASSERT(success, msg);

    // Clean up: Destroy all shared memory regions
    tm_destroy(shared1);
    tm_destroy(shared2);

}

// ------------------------------------- TM_SIZE -------------------------------------

void unit_tests_tm_size() {
    printf("\n----------------------- TM_SIZE ------------------------\n");
    unit_test_tm_size_valid_single();
    unit_test_tm_size_valid_concurrent();
}

void unit_test_tm_size_valid_single() {
    printf("\nRunning unit test for tm_size with valid shared region...\n");
    size_t size = 64;
    size_t align = 16;
    shared_t shared_region = tm_create(size, align);
    size_t segment_size = tm_size(shared_region);
    TEST_ASSERT(segment_size == size, "tm_size success with valid shared region");

    // The returned size must be a multiple of the shared region alignment.
    TEST_ASSERT(segment_size % align == 0, "tm_size returns size multiple of alignment");

    tm_destroy(shared_region);
}

void* thread_tm_size(void* arg) {
    thread_data_t* data = (thread_data_t*)arg;
    data->size = tm_size(data->shared_region);
    return NULL;
}

void unit_test_tm_size_valid_concurrent() {
    printf("\nRunning unit test for tm_size in concurrent manner...\n");

    pthread_t threads[NUM_THREADS];
    thread_data_t thread_data[NUM_THREADS];

    size_t size1 = 32;
    size_t align1 = 16;

    size_t size2 = 64;
    size_t align2 = 32;

    shared_t shared1 = tm_create(size1, align1);
    shared_t shared2 = tm_create(size2, align2);


    // Initialize thread data and create threads
    bool success = true;
    for (int i = 0; i < NUM_THREADS; i++) {
        thread_data[i].size = i % 2 == 0 ? size1 : size2;
        thread_data[i].align = i % 2 == 0 ? align1 : align2;
        thread_data[i].shared_region = i % 2 == 0 ? shared1 : shared2;
        thread_data[i].id = i;

        int rc = pthread_create(&threads[i], NULL, thread_tm_size, (void*)&thread_data[i]);
        if (rc != 0) {
            success = false;
        }
    }
    TEST_ASSERT(success, "pthread_create success multiple threads");

    // Wait for all threads to finish
    for (int i = 0; i < NUM_THREADS; i++) {
        int rc = pthread_join(threads[i], NULL);
        if (rc != 0) {
            success = false;
        }
    }
    TEST_ASSERT(success, "pthread_join success multiple threads");

    // The returned size must be the same as the size passed to tm_create.
    char msg[100] = "size is correct for each thread";
    for (int i = 0; i < NUM_THREADS; i++) {
        size_t segment_size = thread_data[i].size;
        size_t size = i % 2 == 0 ? size1 : size2;
        if (segment_size != size) {
            printf("size: %ld\n", size);
            printf("segment_size: %ld\n", segment_size);
            success = false;
            snprintf(msg, sizeof(msg), "tm_size returns incorrect size in thread %d", i);
            break;
        }
    }
    TEST_ASSERT(success, msg);

    // Clean up: Destroy all shared memory regions
    tm_destroy(shared1);
    tm_destroy(shared2);
}

// ------------------------------------- TM_ALIGN -------------------------------------

void unit_tests_tm_align() {
    printf("\n----------------------- TM_ALIGN ------------------------\n");
    unit_test_tm_align_valid_single();
    unit_test_tm_align_valid_concurrent();
}

void unit_test_tm_align_valid_single() {
    printf("\nRunning unit test for tm_align with valid shared region...\n");
    size_t size = 32;
    size_t align = 16;
    shared_t shared_region = tm_create(size, align);
    size_t alignment = tm_align(shared_region);
    TEST_ASSERT(alignment == align, "tm_align success with valid shared region");

    tm_destroy(shared_region);
}

void* thread_tm_align(void* arg) {
    thread_data_t* data = (thread_data_t*)arg;
    data->align = tm_align(data->shared_region);
    return NULL;
}

void unit_test_tm_align_valid_concurrent() {
    printf("\nRunning unit test for tm_align in concurrent manner...\n");

    pthread_t threads[NUM_THREADS];
    thread_data_t thread_data[NUM_THREADS];

    size_t size1 = 32;
    size_t align1 = 16;

    size_t size2 = 64;
    size_t align2 = 32;

    shared_t shared1 = tm_create(size1, align1);
    shared_t shared2 = tm_create(size2, align2);

    // Initialize thread data and create threads
    bool success = true;
    for (int i = 0; i < NUM_THREADS; i++) {
        thread_data[i].size = i % 2 == 0 ? size1 : size2;
        thread_data[i].align = i % 2 == 0 ? align1 : align2;
        thread_data[i].shared_region = i % 2 == 0 ? shared1 : shared2;
        thread_data[i].id = i;

        int rc = pthread_create(&threads[i], NULL, thread_tm_align, (void*)&thread_data[i]);
        if (rc != 0) {
            success = false;
        }
    }
    TEST_ASSERT(success, "pthread_create success multiple threads");

    // Wait for all threads to finish
    for (int i = 0; i < NUM_THREADS; i++) {
        int rc = pthread_join(threads[i], NULL);
        if (rc != 0) {
            success = false;
        }
    }
    TEST_ASSERT(success, "pthread_join success multiple threads");

    // The returned alignment must be the same as the alignment passed to tm_create.
    char msg[100] = "alignment is correct for each thread";
    for (int i = 0; i < NUM_THREADS; i++) {
        size_t alignment = thread_data[i].align;
        size_t align = i % 2 == 0 ? align1 : align2;
        if (alignment != align) {
            success = false;
            snprintf(msg, sizeof(msg), "tm_align returns incorrect alignment in thread %d", i);
            break;
        }
    }
    TEST_ASSERT(success, msg);

    // Clean up: Destroy all shared memory regions
    tm_destroy(shared1);
    tm_destroy(shared2);
}

// -------------------------------- TM_BEGIN & TM_END --------------------------------

void unit_tests_tm_begin() {
    printf("\n------------------ TM_BEGIN & TM_END -------------------\n");
    unit_test_tm_begin_ro_single();
    unit_test_tm_begin_rw_single();
    unit_test_tm_begin_end_concurrent_one_shared();
}

void unit_test_tm_begin_ro_single() {
    printf("\nRunning unit test for tm_begin with read-only transaction in single thread...\n");
    size_t size = 32;
    size_t align = 16;
    shared_t shared_region = tm_create(size, align);
    tx_t tx = tm_begin(shared_region, true);
    TEST_ASSERT(tx != invalid_tx, "tm_begin success with read-only transaction");

    // Verify that the transaction is read-only
    struct transaction* transaction = (struct transaction*)tx;
    TEST_ASSERT(transaction->is_ro, "tm_begin returns read-only transaction");

    tm_end(shared_region, tx);
    tm_destroy(shared_region);
}

void unit_test_tm_begin_rw_single() {
    printf("\nRunning unit test for tm_begin with read-write transaction in single thread...\n");
    size_t size = 32;
    size_t align = 16;
    shared_t shared_region = tm_create(size, align);
    tx_t tx = tm_begin(shared_region, false);
    TEST_ASSERT(tx != invalid_tx, "tm_begin success with read-write transaction");

    // Verify that the transaction is read-write
    struct transaction* transaction = (struct transaction*)tx;
    TEST_ASSERT(!transaction->is_ro, "tm_begin returns read-write transaction");

    tm_end(shared_region, tx);
    tm_destroy(shared_region);
}

void* thread_tm_begin_end(void* arg) {
    // Sleep for a while to allow multiple epochs to be created
    usleep((rand() % 1000) * 1000);

    thread_data_t* data = (thread_data_t*)arg;
    data->tx = tm_begin(data->shared_region, data->id % 2 == 0);

    // Sleep for a while to allow simulate some work (random time between 0 and 1 second)
    usleep((rand() % 1000) * 1000);

    tm_end(data->shared_region, data->tx);
    return NULL;
}

void* thread_tm_begin_rw(void* arg) {
    thread_data_t* data = (thread_data_t*)arg;
    data->tx = tm_begin(data->shared_region, false);
    return NULL;
}

void unit_test_tm_begin_end_concurrent_one_shared() {
    printf("\nRunning unit test for tm_begin with read-only transaction in concurrent manner...\n");

    pthread_t threads[NUM_THREADS];
    thread_data_t thread_data[NUM_THREADS];

    size_t size = 32;
    size_t align = 16;
    shared_t shared_region = tm_create(size, align);

    // Initialize thread data and create threads
    bool success = true;
    for (int i = 0; i < NUM_THREADS; i++) {
        thread_data[i].size = size;
        thread_data[i].align = align;
        thread_data[i].shared_region = shared_region;
        thread_data[i].tx = invalid_tx;
        thread_data[i].id = i;

        int rc = pthread_create(&threads[i], NULL, thread_tm_begin_end, (void*)&thread_data[i]);
        if (rc != 0) {
            success = false;
        }
    }
    TEST_ASSERT(success, "pthread_create success multiple threads");

    // Wait for all threads to finish
    // Timout after 1 minute to avoid infinite loop

    for (int i = 0; i < NUM_THREADS; i++) {
        int rc = pthread_join(threads[i], NULL);
        if (rc != 0) {
            success = false;
        }
    }
    TEST_ASSERT(success, "pthread_join success multiple threads");

    // Clean up: Destroy the shared memory region
    tm_destroy(shared_region);
}

// ------------------------------------- TM_READ -------------------------------------

void unit_tests_tm_read() {
    printf("\n----------------------- TM_READ ------------------------\n");
    unit_test_tm_read_ro_single();
    unit_test_tm_read_rw_single();
    unit_test_tm_read_concurrent();
}

void unit_test_tm_read_ro_single() {
    printf("\nRunning unit test for tm_read with read-only transaction in single thread...\n");

    // Initialize the shared memory region
    size_t size = 32;
    size_t align = 16;
    shared_t shared_region = tm_create(size, align);

    // Begin a read-only transaction
    tx_t tx = tm_begin(shared_region, true);

    // Read the first word of the segment
    size_t read_size = 16;
    uint8_t* word = malloc(read_size);
    memset(word, 1, read_size);

    void* start = tm_start(shared_region);
    bool success = tm_read(shared_region, tx, start, read_size, word);
    TEST_ASSERT(success, "tm_read success with read-only transaction");

    // The read value must be zero
    uint8_t* expected_output = malloc(read_size);
    memset(expected_output, 0, read_size);

    // The read value must be equal to the expected output
    bool equal = memcmp(word, expected_output, read_size) == 0;
    TEST_ASSERT(equal, "tm_read returns correct value");

    tm_end(shared_region, tx);
    tm_destroy(shared_region);
    free(word);
}

void unit_test_tm_read_rw_single() {
    printf("\nRunning unit test for tm_read with read-write transaction in single thread...\n");

    // Initialize the shared memory region
    size_t size = 64;
    size_t align = 16;
    shared_t shared_region = tm_create(size, align);

    // Begin a read-write transaction
    tx_t tx = tm_begin(shared_region, false);

    // Read the first word of the segment
    size_t word_offset = 1;
    size_t offset = word_offset * align;
    size_t word_read_size = 2;
    size_t read_size = word_read_size * align;
    uint8_t* word = malloc(read_size);
    memset(word, 1, read_size);

    void* start = tm_start(shared_region);
    bool success = tm_read(shared_region, tx, start + offset, read_size, word);
    TEST_ASSERT(success, "tm_read success with read-write transaction");

    // The read value must be zero
    uint8_t* expected_output = malloc(read_size);
    memset(expected_output, 0, read_size);

    // The read value must be equal to the expected output
    bool equal = memcmp(word, expected_output, read_size) == 0;
    TEST_ASSERT(equal, "tm_read returns correct value");

    // Verify that the control structure is correct
    dual_segment* segment = ((region*)shared_region)->segment_head;
    for (size_t i = 0; i < word_read_size; i++) {
        controls* control = &(segment->controls[word_offset + i]);
        if (control->written_while_epoch || control->access_set != tx) {
            success = false;
            break;
        }
    }
    TEST_ASSERT(success, "tm_read updates the control structure correctly");

    tm_end(shared_region, tx);
    tm_destroy(shared_region);
    free(word);
}

void* thread_tm_read(void* arg) {
    thread_data_t* data = (thread_data_t*)arg;

    // Sleep for a while to allow multiple epochs to be created
    usleep((rand() % 1000) * 1000);

    data->tx = tm_begin(data->shared_region, data->id % 2 == 0);

    // Sleep for a while to allow simulate some work (random time between 0 and 1 second)
    usleep((rand() % 1000) * 1000);

    size_t word_offset = data->read_offset;
    size_t offset = word_offset * data->align;
    size_t word_read_size = data->read_size;
    size_t read_size = word_read_size * data->align;
    uint8_t* word = malloc(read_size);
    memset(word, 1, read_size);

    void* start = tm_start(data->shared_region);
    bool success = tm_read(data->shared_region, data->tx, start + offset, read_size, word);
    if (!success) {
        return NULL;
    }

    // The read value must be zero
    uint8_t* expected_output = malloc(read_size);
    memset(expected_output, 0, read_size);

    // The read value must be equal to the expected output
    bool equal = memcmp(word, expected_output, read_size) == 0;
    if (!equal) {
        TEST_ASSERT(equal, "tm_read returns correct value");
    }

    tm_end(data->shared_region, data->tx);

    return NULL;
}

void unit_test_tm_read_concurrent() {
    printf("\nRunning unit test for tm_read in concurrent manner...\n");

    pthread_t threads[NUM_THREADS];
    thread_data_t thread_data[NUM_THREADS];

    size_t size = 64;
    size_t align = 16;
    shared_t shared_region = tm_create(size, align);

    size_t read_size = 2;
    size_t read_offset = 1;

    // Initialize thread data and create threads
    bool success = true;
    for (int i = 0; i < NUM_THREADS; i++) {
        thread_data[i].size = size;
        thread_data[i].align = align;
        thread_data[i].shared_region = shared_region;
        thread_data[i].tx = invalid_tx;
        thread_data[i].id = i;
        thread_data[i].read_size = read_size;
        thread_data[i].read_offset = read_offset;

        int rc = pthread_create(&threads[i], NULL, thread_tm_read, (void*)&thread_data[i]);
        if (rc != 0) {
            success = false;
        }
    }
    TEST_ASSERT(success, "pthread_create success multiple threads");

    // Wait for all threads to finish
    for (int i = 0; i < NUM_THREADS; i++) {
        int rc = pthread_join(threads[i], NULL);
        if (rc != 0) {
            success = false;
        }
    }
    TEST_ASSERT(success, "pthread_join success multiple threads");

    // Clean up: Destroy the shared memory region
    tm_destroy(shared_region);
}

// ------------------------------------- TM_WRITE -------------------------------------

void unit_tests_tm_write() {
    printf("\n----------------------- TM_WRITE ------------------------\n");
    unit_test_tm_write_single();
    //unit_test_tm_write_two();
    //unit_test_tm_write_concurrent();
}

void unit_test_tm_write_single() {
    printf("\nRunning unit test for tm_write in single thread...\n");

    // Initialize the shared memory region
    size_t size = 48;
    size_t align = 16;
    shared_t shared_region = tm_create(size, align);

    // Begin a read-write transaction
    tx_t tx = tm_begin(shared_region, false);

    // Write the first word of the segment
    size_t word_offset = 1;
    size_t offset = word_offset * align;
    size_t word_write_size = 2;
    size_t write_size = word_write_size * align;
    uint8_t* word = malloc(write_size);
    memset(word, 1, write_size); // Write 1 to the word

    void* start = tm_start(shared_region);
    bool success = tm_write(shared_region, tx, word, write_size, start + offset);
    TEST_ASSERT(success, "tm_write success with read-write transaction");

    // The read value must be equal to the written value
    uint8_t* read_word = malloc(write_size);
    memset(read_word, 0, write_size);
    success = tm_read(shared_region, tx, start + offset, write_size, read_word);
    TEST_ASSERT(success, "tm_read success after tm_write");

    // The read value must be equal to the written value
    bool equal = memcmp(word, read_word, write_size) == 0;
    TEST_ASSERT(equal, "tm_read returns correct value after tm_write");

    // Verify that the control structure is correct
    dual_segment* segment = ((region*)shared_region)->segment_head;
    for (size_t i = 0; i < word_write_size; i++) {
        controls* control = &(segment->controls[word_offset + i]);
        if (!control->written_while_epoch || control->access_set != tx) {
            success = false;
            break;
        }
    }
    TEST_ASSERT(success, "tm_write updates the control structure correctly");

    // Verify that the values around the written word are not changed
    for (size_t i = 0; i < size / align; i++) {
        if (i < word_offset || i >= word_offset + word_write_size) {
            uint8_t* read_word = malloc(align);
            memset(read_word, 0, align);
            success = tm_read(shared_region, tx, start + i * align, align, read_word);
            if (!success) {
                break;
            }
            bool equal = memcmp(read_word, 0, align) == 0;
            if (!equal) {
                success = false;
                break;
            }
            free(read_word);
        }
    }
    TEST_ASSERT(success, "tm_write does not change other values");

    tm_end(shared_region, tx);
    tm_destroy(shared_region);
    free(word);
    free(read_word);

}




// --------------------------------------- MAIN ---------------------------------------

int main() {
    // unit_tests_tm_create();
    // unit_tests_tm_destroy();
    // unit_tests_tm_start();
    // unit_tests_tm_size();
    // unit_tests_tm_align();
    // unit_tests_tm_begin();
    // unit_tests_tm_read();
    unit_tests_tm_write();
    return 0;
}