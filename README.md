# Software Transactional Memory in C11

A software transactional memory (STM) library written in C11, implementing the
[`tm.h`](include/tm.h) interface of the EPFL **CS-453 — Concurrent Algorithms** course project.
It is built as a shared object and loaded by the course's grading harness, which
checks correctness against a reference implementation and measures speedup.

The implementation lives in [`346328/tm.c`](346328/tm.c) (~990 lines). Everything else in
this repository is course-provided material: the interface headers, a coarse-grained
reference implementation, the grading program, and synchronization examples.

## Design

The design is **dual-versioned memory driven by a batcher**, the approach outlined in the
project description. Transactions run in batches ("epochs"); within an epoch they operate on
a private writable copy of memory, and all writes become visible atomically when the epoch ends.

### Batcher — epoch management

A single `batcher_data` structure serializes entry into epochs with a mutex and two condition
variables. Read-only transactions enter freely. Read-write transactions block until the current
epoch drains, then are woken as a group, so every transaction in an epoch sees the same snapshot.
The last transaction to leave an epoch (`remaining == 0`) performs the commit and bumps the epoch
counter, waking either the blocked group or newly arriving transactions.

### Dual-versioned segments

Each `dual_segment` holds two byte arrays of equal size:

| Copy | Role |
| --- | --- |
| `ro_words` | The committed snapshot, read by all transactions in the epoch |
| `rw_words` | The working copy, mutated in place by writers |

Alongside them is a per-word `controls` array carrying a `pthread_rwlock_t`, the owning
transaction, and a `written_while_epoch` flag. Word granularity is the region's alignment.

### Read / write protocol

- **Read-only transactions** copy straight out of `ro_words` with no locking at all — they can
  never conflict, since `ro_words` is immutable for the duration of the epoch.
- **Read-write transactions** take per-word read locks, then abort if a word was written this
  epoch by *another* transaction. A word the transaction wrote itself is read back from
  `rw_words`, so a transaction always observes its own writes.
- **Writes** take per-word write locks and abort if the word is already owned by another
  transaction. Otherwise the previous value is pushed onto a per-transaction **undo log**, the
  word is marked owned, and `rw_words` is updated in place.

### Commit and rollback

Because writers mutate `rw_words` directly, an abort is undone by replaying the undo log
backwards into `rw_words`, restoring the pre-transaction bytes. Commit is deferred to the end
of the epoch: the last transaction out copies `rw_words` over `ro_words` for every segment and
clears the per-word ownership flags in one pass. There is no per-transaction commit-time
validation — conflicts are caught eagerly, at the moment of the conflicting access.

### Allocation

`tm_alloc` appends a new dual segment to a linked list and returns the address of its read-only
copy, which is what the interface exposes to callers. `tm_free` unlinks a segment under the
region's segment mutex; the initial segment created by `tm_create` is flagged non-freeable.

## Build and run

Build the library (produces `346328.so` at the repository root):

```sh
make -C 346328 build
```

Build and run the grading harness, which compares the implementation against the reference
and reports the speedup:

```sh
make -C grading build-libs LIB_DIRS="../reference/ ../346328/"
make -C grading build
make -C grading run LIB_DIRS="../reference/ ../346328/"
```

The harness takes a seed, the reference library, and one or more libraries to test:

```sh
./grading/grading 453 reference.so 346328.so
```

It spawns one worker per hardware thread, replays randomized transactional workloads, and
fails loudly on any consistency violation. The final speedup figure is relative to the
coarse-grained reference in [`reference/`](reference/).

## Repository layout

| Path | Contents |
| --- | --- |
| [`346328/`](346328/) | The implementation — `tm.c` plus its build rules |
| [`include/`](include/) | The STM interface the library must satisfy (`tm.h`, `tm.hpp`) |
| [`reference/`](reference/) | Course-provided reference implementation using a single shared lock |
| [`grading/`](grading/) | Correctness and performance harness used for evaluation |
| [`playground/`](playground/) | Minimal driver for experimenting with a library |
| [`sync-examples/`](sync-examples/) | Course examples of C11 synchronization primitives |
| `submit.py` | Course submission script |

## Notes

- The library depends only on `pthread` and C11 atomics; it is compiled with
  `-Ofast -march=native` via [`346328/Makefile`](346328/Makefile).
- `346328` is the submission directory name required by the course (a student identifier),
  which is why it also determines the name of the produced shared object.
