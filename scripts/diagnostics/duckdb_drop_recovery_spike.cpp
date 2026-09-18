/* EXPERIMENT ONLY: tiny-fixture DROP interposition, never a server extension. */
#include <dlfcn.h>
#include <atomic>
#include <string>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>

using std::atomic_store;
using std::atomic_load;
using std::atomic_fetch_add;
static std::atomic<int> armed{0};
static std::atomic<int> intercepted{0};
static std::atomic<int> fresh_metadata{0};
static std::atomic<int> fresh_allocations{0};
static std::atomic<int> crash_after_flush{0};
static std::atomic<int> entry_calls{0};
static thread_local bool allowed_table_drop = false;

extern "C" {

void lh_spike_arm(void) { atomic_store(&armed, 1); }
int lh_spike_count(void) { return atomic_load(&intercepted); }
void lh_spike_disarm(void) { atomic_store(&armed, 0); }
void lh_spike_checkpoint(int enable) { atomic_store(&fresh_metadata, enable); }
int lh_spike_allocations(void) { return atomic_load(&fresh_allocations); }
int lh_spike_entry_calls(void) { return atomic_load(&entry_calls); }
void lh_spike_crash_flush(void) { atomic_store(&crash_after_flush, 1); }

void lh_table_drop(void *table)
    __asm__("_ZN6duckdb14DuckTableEntry10CommitDropEv");

void lh_table_drop(void *table) {
    atomic_fetch_add(&entry_calls, 1);
    auto original = reinterpret_cast<void (*)(void *)>(dlsym(RTLD_NEXT,
        "_ZN6duckdb14DuckTableEntry10CommitDropEv"));
    auto table_sql = reinterpret_cast<std::string (*)(void *)>(dlsym(RTLD_NEXT,
        "_ZNK6duckdb17TableCatalogEntry5ToSQLB5cxx11Ev"));
    if (!original || !table_sql) {
        _exit(94);
    }
    const bool previous = allowed_table_drop;
    allowed_table_drop = false;
    if (atomic_load(&armed)) {
        const auto sql = table_sql(table);
        const auto message = std::string("synthetic_drop_candidate=") + sql.substr(0, 120) + "\n";
        if (write(2, message.data(), message.size()) < 0) {
            _exit(96);
        }
        // Exact catalog, schema and reserved identity, observed on stock 1.5.4.
        // Never broaden this to a first-DROP or substring match.
        const std::string prefix = "CREATE TABLE candidate.public.__corrupt_recovery_player_fantasy_season(";
        allowed_table_drop = sql.compare(0, prefix.size(), prefix) == 0;
    }
    try {
        original(table);
    } catch (...) {
        allowed_table_drop = previous;
        throw;
    }
    allowed_table_drop = previous;
}

void lh_metadata_flush(void *manager)
    __asm__("_ZN6duckdb15MetadataManager5FlushEv");

void lh_metadata_flush(void *manager) {
    auto original = reinterpret_cast<void (*)(void *)>(dlsym(RTLD_NEXT,
        "_ZN6duckdb15MetadataManager5FlushEv"));
    if (!original) {
        _exit(95);
    }
    original(manager);
    if (atomic_load(&crash_after_flush)) {
        _exit(24);
    }
}

int64_t lh_metadata_peek(void *manager)
    __asm__("_ZNK6duckdb15MetadataManager15PeekNextBlockIdEv");

int64_t lh_metadata_peek(void *manager) {
    if (atomic_load(&fresh_metadata)) {
        /* AllocateHandle must allocate a new metadata block, not pin an old
         * partially free one. Normal allocation and checksums stay intact. */
        atomic_fetch_add(&fresh_allocations, 1);
        return -1;
    }
    auto original = reinterpret_cast<int64_t (*)(void *)>(dlsym(RTLD_NEXT,
        "_ZNK6duckdb15MetadataManager15PeekNextBlockIdEv"));
    if (!original) {
        _exit(93);
    }
    return original(manager);
}

void lh_collection_drop(void *collection)
    __asm__("_ZN6duckdb18RowGroupCollection15CommitDropTableEv");

void lh_collection_drop(void *collection) {
    if (allowed_table_drop) {
        atomic_fetch_add(&intercepted, 1);
        return; /* Retain allocated blocks. Do not inspect or rewrite data. */
    }
    auto original = reinterpret_cast<void (*)(void *)>(dlsym(RTLD_NEXT,
        "_ZN6duckdb18RowGroupCollection15CommitDropTableEv"));
    if (!original) {
        const char message[] = "spike: original drop symbol unavailable\n";
        if (write(2, message, sizeof(message) - 1) < 0) {
            _exit(92);
        }
        _exit(91);
    }
    original(collection);
}
} // extern "C"
