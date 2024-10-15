#include <stdio.h>
#include <unistd.h>  // For sleep
#include <pthread.h> // For pthread_create and pthread_join
#include <stdlib.h>  // For exit
#include <stdbool.h> // For bool type
#include "tm.h"      // Include your tm.h file

#define NUM_THREADS 50

// Macro for testing
#define TEST_ASSERT(cond, msg) \
    if (!(cond)) { \
        fprintf(stderr, "Test failed: %s\n", msg); \
        exit(EXIT_FAILURE); \
    } else { \
        printf("Test passed: %s\n", msg); \
    }


void test_single_thread() {
    printf("Running single thread test...\n");
    shared_t shared_region = tm_create(1024, 16); // Assume `tm_create` initializes a shared region
    TEST_ASSERT(shared_region != NULL, "Shared memory region created");

    // Begin a read-only transaction
    tx_t tx = tm_begin(shared_region, true);
    TEST_ASSERT(tx != invalid_tx, "Begin read-only transaction");

    // End the transaction
    bool commit = tm_end(shared_region, tx);
    TEST_ASSERT(commit == true, "End transaction successfully");

    tm_destroy(shared_region); // Clean up the memory
}

void* thread_func(void* arg) {
    shared_t shared_region = (shared_t)arg;
    tx_t tx = tm_begin(shared_region, false);
    TEST_ASSERT(tx != invalid_tx, "Thread began transaction");

    // Perform some work here (simulate)
    usleep(100000 + rand() % 400000);

    bool commit = tm_end(shared_region, tx);
    TEST_ASSERT(commit == true, "Thread ended transaction successfully");

    return NULL;
}

void test_multi_thread() {
    pthread_t threads[NUM_THREADS];
    shared_t shared_region = tm_create(1024, 16);
    TEST_ASSERT(shared_region != NULL, "Shared memory region created");

    // Create multiple threads
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_create(&threads[i], NULL, thread_func, shared_region);
    }

    // Wait for all threads to finish
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    tm_destroy(shared_region); // Clean up the memory
}

void* batcher_test_func(void* arg) {
    shared_t shared_region = (shared_t)arg;
    tx_t tx = tm_begin(shared_region, false);

    // Simulate some transaction work
    sleep(10);

    tm_end(shared_region, tx);
    return NULL;
}

void test_batcher() {
    pthread_t threads[NUM_THREADS];
    shared_t shared_region = tm_create(1024, 16);

    // Create multiple threads
    for (int i = 0; i < NUM_THREADS; i++) {
        // Wait between 100ms and 500ms
        usleep(100000 + rand() % 400000);
        pthread_create(&threads[i], NULL, batcher_test_func, shared_region);
    }

    // Wait for all threads to finish
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    tm_destroy(shared_region); // Clean up the memory
}

void run_tests() {
    printf("Running tests...\n");
    // test_single_thread();

    //test_multi_thread();
    test_batcher();
}

int main() {
    run_tests();
    return 0;
}
