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

void unit_tests_tm_create() {
    printf("\nRunning unit test for tm_create...\n");
    unit_test_tm_create_invalid_size();
    unit_test_tm_create_invalid_alignment();
    unit_test_tm_create_valid();
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
}

void unit_test_tm_create_valid() {
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

    // This function can be called concurrently.


}

int main() {
    unit_tests_tm_create();
    return 0;
}