/* Offline recovery experiment, never a server extension. The real-file build
 * is separately tested; loading it does not arm any destructive behavior. */
#include <dlfcn.h>
#include <atomic>
#include <string>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#ifdef LH_REAL_FILE
#include <chrono>
#include <mutex>
#include <cstring>
static std::mutex progress_mutex;
static std::string progress_table;
static bool progress_active = false;
static unsigned progress_completed = 0;
static std::chrono::steady_clock::time_point progress_started;
#endif

using std::atomic_store;
using std::atomic_load;
using std::atomic_fetch_add;
static std::atomic<int> armed{0};
static std::atomic<int> intercepted{0};
static std::atomic<int> fresh_metadata{0};
static std::atomic<int> fresh_allocations{0};
// Never turn a narrow catalog experiment into an unbounded metadata rewrite.
static constexpr int max_metadata_blocks = 128;
static std::atomic<int> allocation_limit{max_metadata_blocks};
static std::atomic<int> crash_after_flush{0};
static std::atomic<int> entry_calls{0};
static std::atomic<int64_t> reserved_metadata_block{-1};
static std::atomic<int> reserved_mask_calls{0};
static thread_local bool allowed_table_drop = false;

// Built only by the synthetic shared-reference case, never the real-file helper.
#ifdef LH_TEST_MARK_ORIGIN
#ifdef LH_REAL_FILE
#error "Mark-origin instrumentation is synthetic-only"
#endif
static thread_local bool inside_metadata_mark = false;
static std::atomic<int> metadata_mark_calls{0};
static std::atomic<int> metadata_mark_partial_calls{0};
static std::atomic<uint64_t> metadata_mark_partial_mask{0};
#elif defined(LH_TEST_SKIP_MARK_RESERVATION)
#error "The Mark-mask mutant requires synthetic origin instrumentation"
#endif

extern "C" {

#ifdef LH_REAL_FILE
// Observation only. The pinned stock method is always called, unmodified.
// A Python observer samples this bounded state; no logging or fsync occurs
// on the engine's checkpoint path and no row/column values are exposed.
int lh_spike_checkpoint_progress(char *buffer, int capacity) {
    std::lock_guard<std::mutex> guard(progress_mutex);
    auto elapsed = progress_active ? std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - progress_started).count() : 0;
    const auto message = std::string("{\"event\":\"checkpoint_table_progress\",\"table\":\"") +
        progress_table + "\",\"active\":" + (progress_active ? "true" : "false") +
        ",\"completed_tables\":" + std::to_string(progress_completed) +
        ",\"elapsed_ms\":" + std::to_string(elapsed) + "}";
    if (capacity < 1 || message.size() >= static_cast<size_t>(capacity)) { return -1; }
    std::memcpy(buffer, message.c_str(), message.size() + 1);
    return static_cast<int>(message.size());
}

void lh_checkpoint_table(void *writer, void *table, void *serializer)
    __asm__("_ZN6duckdb26SingleFileCheckpointWriter10WriteTableERNS_17TableCatalogEntryERNS_10SerializerE");

void lh_checkpoint_table(void *writer, void *table, void *serializer) {
    auto original = reinterpret_cast<void (*)(void *, void *, void *)>(dlsym(RTLD_NEXT,
        "_ZN6duckdb26SingleFileCheckpointWriter10WriteTableERNS_17TableCatalogEntryERNS_10SerializerE"));
    if (!original) { _exit(86); }
#ifdef LH_TEST_SKIP_CHECKPOINT_OBSERVER
    original(writer, table, serializer); // Synthetic negative control only.
#else
    if (!atomic_load(&fresh_metadata)) {
        original(writer, table, serializer);
        return;
    }
    auto storage = reinterpret_cast<void *(*)(void *)>(dlsym(RTLD_NEXT,
        "_ZN6duckdb14DuckTableEntry10GetStorageEv"));
    auto table_name = reinterpret_cast<std::string (*)(void *)>(dlsym(RTLD_NEXT,
        "_ZNK6duckdb9DataTable12GetTableNameB5cxx11Ev"));
    if (!storage || !table_name) { _exit(85); }
    std::string name = "redacted";
    try {
        // GetTableName copies only the name, never full column/constraint DDL.
        const auto candidate = table_name(storage(table));
        if (candidate.size() <= 128 &&
            candidate.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.") == std::string::npos) {
            name = candidate;
        }
    } catch (...) {
        // Diagnostic name lookup must not suppress the stock checkpoint.
    }
    {
        std::lock_guard<std::mutex> guard(progress_mutex);
        progress_table = name;
        progress_active = true;
        progress_started = std::chrono::steady_clock::now();
    }
    try {
        original(writer, table, serializer);
    } catch (...) {
        std::lock_guard<std::mutex> guard(progress_mutex);
        progress_active = false;
        throw;
    }
    std::lock_guard<std::mutex> guard(progress_mutex);
    progress_active = false;
    ++progress_completed;
#endif
}
#endif

#ifdef LH_TEST_MARK_ORIGIN
int lh_spike_test_mark_calls(void) { return atomic_load(&metadata_mark_calls); }
int lh_spike_test_mark_partial_calls(void) { return atomic_load(&metadata_mark_partial_calls); }
uint64_t lh_spike_test_mark_partial_mask(void) { return atomic_load(&metadata_mark_partial_mask); }

void lh_test_metadata_mark(void *manager)
    __asm__("_ZN6duckdb15MetadataManager20MarkBlocksAsModifiedEv");

void lh_test_metadata_mark(void *manager) {
    auto original = reinterpret_cast<void (*)(void *)>(dlsym(RTLD_NEXT,
        "_ZN6duckdb15MetadataManager20MarkBlocksAsModifiedEv"));
    if (!original) { _exit(87); }
    const bool previous = inside_metadata_mark;
    inside_metadata_mark = true;
    atomic_fetch_add(&metadata_mark_calls, 1);
    try {
        original(manager);
    } catch (...) {
        inside_metadata_mark = previous;
        throw;
    }
    inside_metadata_mark = previous;
}
#endif

void lh_spike_arm(void) { atomic_store(&armed, 1); }
// Startup replay may legitimately contain earlier unrelated DROP records.
// Those keep stock behavior; explicit removal switches back to strict mode.
void lh_spike_replay(void) { atomic_store(&armed, 2); }
int lh_spike_count(void) { return atomic_load(&intercepted); }
void lh_spike_disarm(void) { atomic_store(&armed, 0); }
void lh_spike_checkpoint(int enable) { atomic_store(&fresh_metadata, enable); }
int lh_spike_allocations(void) { return atomic_load(&fresh_allocations); }
void lh_spike_allocation_limit(int limit) {
    if (limit < 1 || limit > max_metadata_blocks) { _exit(97); }
    atomic_store(&allocation_limit, limit);
}
int lh_spike_entry_calls(void) { return atomic_load(&entry_calls); }
void lh_spike_crash_flush(void) { atomic_store(&crash_after_flush, 1); }
void lh_spike_avoid_block(int64_t id) {
    if (id < -1 || id >= (int64_t(1) << 62)) { _exit(90); }
    atomic_store(&reserved_metadata_block, id);
}
int lh_spike_reserved_masks(void) { return atomic_load(&reserved_mask_calls); }

void lh_metadata_free_mask(void *block, uint64_t mask)
    __asm__("_ZN6duckdb13MetadataBlock21FreeBlocksFromIntegerEm");

void lh_metadata_free_mask(void *block, uint64_t mask) {
    auto original = reinterpret_cast<void (*)(void *, uint64_t)>(dlsym(RTLD_NEXT,
        "_ZN6duckdb13MetadataBlock21FreeBlocksFromIntegerEm"));
    if (!original) { _exit(89); }
    const auto reserved = atomic_load(&reserved_metadata_block);
    if (reserved >= 0) {
        auto describe = reinterpret_cast<std::string (*)(void *)>(dlsym(RTLD_NEXT,
            "_ZNK6duckdb13MetadataBlock8ToStringB5cxx11Ev"));
        if (!describe) { _exit(88); }
        const auto prefix = std::string("block_id: ") + std::to_string(reserved) + " [";
        if (describe(block).compare(0, prefix.size(), prefix) == 0) {
#ifdef LH_TEST_MARK_ORIGIN
            // Count the ORIGINAL nontrivial mask only while the real engine's
            // MarkBlocksAsModified is on-stack, not a direct test invocation.
            if (inside_metadata_mark && mask != 0 && mask != UINT64_MAX) {
                atomic_fetch_add(&metadata_mark_partial_calls, 1);
                atomic_store(&metadata_mark_partial_mask, mask);
            }
#endif
            // Reserve this block's free subslots without pinning/reading it.
            // Live pointers remain live; normal all-unreferenced retirement
            // happens before FreeBlocksFromInteger in MarkBlocksAsModified.
#ifdef LH_TEST_SKIP_MARK_RESERVATION
            // Negative control: Read still reserves; only the Mark path leaks.
            if (!inside_metadata_mark) { mask = 0; }
#else
            mask = 0;
#endif
            atomic_fetch_add(&reserved_mask_calls, 1);
        }
    }
    original(block, mask);
}

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
#ifdef LH_REAL_FILE
        const char *names[] = {
            "homepage_manager_rankings", "matchup_h2h_career", "player_fantasy_season",
            "player_fantasy_season_all", "standings_by_year"
        };
        for (const auto name : names) {
            const auto prefix = std::string("CREATE TABLE ___leagues.public.__corrupt_recovery_") + name + "(";
            if (sql.compare(0, prefix.size(), prefix) == 0) {
                allowed_table_drop = true;
                break;
            }
        }
        const std::string matchup_prefix =
            "CREATE TABLE ___leagues.public.__replaced_matchup_season_1790022766492049920(";
        if (sql.compare(0, matchup_prefix.size(), matchup_prefix) == 0) {
            allowed_table_drop = true;
        }
        // Explicit removal is fail-closed. Never forward an unexpected DROP
        // while the real-file helper is armed.
        if (!allowed_table_drop && atomic_load(&armed) == 1) { _exit(99); }
#else
        const std::string prefix = "CREATE TABLE candidate.public.__corrupt_recovery_player_fantasy_season(";
        allowed_table_drop = sql.compare(0, prefix.size(), prefix) == 0;
#endif
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

int64_t lh_metadata_next(void *manager)
    __asm__("_ZNK6duckdb15MetadataManager14GetNextBlockIdEv");

int64_t lh_metadata_next(void *manager) {
    auto original = reinterpret_cast<int64_t (*)(void *)>(dlsym(RTLD_NEXT,
        "_ZNK6duckdb15MetadataManager14GetNextBlockIdEv"));
    if (!original) { _exit(98); }
    // Bound actual fresh block allocation, including the empty-free-list path
    // that does not call PeekNextBlockId at all.
    if (atomic_load(&fresh_metadata) &&
        atomic_fetch_add(&fresh_allocations, 1) >= atomic_load(&allocation_limit)) {
        _exit(97);
    }
    return original(manager);
}

int64_t lh_metadata_peek(void *manager)
    __asm__("_ZNK6duckdb15MetadataManager15PeekNextBlockIdEv");

int64_t lh_metadata_peek(void *manager) {
    // Keep normal 64-subslot packing on healthy blocks. The exact damaged
    // block's free mask is reserved separately, never globally force -1 here.
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
