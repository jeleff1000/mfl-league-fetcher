/* EXPERIMENT ONLY: tiny-fixture DROP interposition, never a server extension. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>

static _Atomic int armed = 0;
static _Atomic int intercepted = 0;
static _Atomic int fresh_metadata = 0;
static _Atomic int fresh_allocations = 0;

void lh_spike_arm(void) { atomic_store(&armed, 1); }
int lh_spike_count(void) { return atomic_load(&intercepted); }
void lh_spike_disarm(void) { atomic_store(&armed, 0); }
void lh_spike_checkpoint(int enable) { atomic_store(&fresh_metadata, enable); }
int lh_spike_allocations(void) { return atomic_load(&fresh_allocations); }

int64_t lh_metadata_peek(void *manager)
    __asm__("_ZNK6duckdb15MetadataManager15PeekNextBlockIdEv");

int64_t lh_metadata_peek(void *manager) {
    if (atomic_load(&fresh_metadata)) {
        /* AllocateHandle must allocate a new metadata block, not pin an old
         * partially free one. Normal allocation and checksums stay intact. */
        atomic_fetch_add(&fresh_allocations, 1);
        return -1;
    }
    int64_t (*original)(void *) = dlsym(RTLD_NEXT,
        "_ZNK6duckdb15MetadataManager15PeekNextBlockIdEv");
    if (!original) {
        _exit(93);
    }
    return original(manager);
}

void lh_collection_drop(void *collection)
    __asm__("_ZN6duckdb18RowGroupCollection15CommitDropTableEv");

void lh_collection_drop(void *collection) {
    if (atomic_exchange(&armed, 0)) {
        atomic_fetch_add(&intercepted, 1);
        return; /* Retain allocated blocks. Do not inspect or rewrite data. */
    }
    void (*original)(void *) = dlsym(RTLD_NEXT,
        "_ZN6duckdb18RowGroupCollection15CommitDropTableEv");
    if (!original) {
        const char message[] = "spike: original drop symbol unavailable\n";
        if (write(2, message, sizeof(message) - 1) < 0) {
            _exit(92);
        }
        _exit(91);
    }
    original(collection);
}
