// Instrumentation ONLY. Compile against the selected vendor's real headers,
// config and flags. No guessed C++ offsets, callbacks, DDS calls or stdout.
// The two C ABI outputs require a valid writable, non-overlapping buffer of
// EXACTLY out_bytes bytes; null/wrong size returns -1 without touching memory.
// Records are completion-order FIFO; produced includes overflow drops, so
// produced == consumed + pending + dropped. Ordinal gaps expose lost records.
// Fork/signal-handler use is not supported; load into a fresh worker process.
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <fastdds/topic/DDSSQLFilter/DDSFilterExpression.hpp>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <dlfcn.h>
#include <pthread.h>
#include <time.h>
#include <unistd.h>

struct r2f_fastdds_filter_trace_record_v1
{
    uint32_t schema_version;
    uint32_t struct_size;
    uint64_t ordinal;
    uint64_t started_ns;
    uint64_t completed_ns;
    uint32_t pid;
    int32_t decision;
    uint32_t status;
    uint32_t payload_length;
    uint32_t payload_copied;
    int32_t sequence_high;
    uint32_t sequence_low;
    uint32_t encapsulation;
    uint8_t reader_guid[16];
    uint8_t writer_guid[16];
    uint8_t payload[4096];
};
struct r2f_fastdds_filter_trace_stats_v1
{
    uint32_t schema_version;
    uint32_t struct_size;
    uint64_t produced;
    uint64_t consumed;
    uint64_t dropped;
    uint64_t pending;
    uint32_t resolve_status;
    uint32_t reserved;
};
#define OFFSET(type, field, expected) static_assert(offsetof(type, field) == expected, #field " offset")
using Record = r2f_fastdds_filter_trace_record_v1;
using Stats = r2f_fastdds_filter_trace_stats_v1;
static_assert(sizeof(Record) == 4192, "record ABI size");
static_assert(sizeof(Stats) == 48, "stats ABI size");
OFFSET(Record, schema_version, 0); OFFSET(Record, struct_size, 4);
OFFSET(Record, ordinal, 8); OFFSET(Record, started_ns, 16);
OFFSET(Record, completed_ns, 24); OFFSET(Record, pid, 32);
OFFSET(Record, decision, 36); OFFSET(Record, status, 40);
OFFSET(Record, payload_length, 44); OFFSET(Record, payload_copied, 48);
OFFSET(Record, sequence_high, 52); OFFSET(Record, sequence_low, 56);
OFFSET(Record, encapsulation, 60); OFFSET(Record, reader_guid, 64);
OFFSET(Record, writer_guid, 80); OFFSET(Record, payload, 96);
OFFSET(Stats, schema_version, 0); OFFSET(Stats, struct_size, 4);
OFFSET(Stats, produced, 8); OFFSET(Stats, consumed, 16);
OFFSET(Stats, dropped, 24); OFFSET(Stats, pending, 32);
OFFSET(Stats, resolve_status, 40); OFFSET(Stats, reserved, 44);
#undef OFFSET

namespace {
using Filter = eprosima::fastdds::dds::DDSSQLFilter::DDSFilterExpression;
using Info = eprosima::fastdds::dds::IContentFilter::FilterSampleInfo;
using Payload = eprosima::fastdds::dds::IContentFilter::SerializedPayload;
using Guid = eprosima::fastdds::dds::IContentFilter::GUID_t;
// GNU/Linux member ABI: hidden const this followed by the THREE typed refs.
using Evaluate = bool (*)(const Filter*, const Payload&, const Info&, const Guid&);
const char kSymbol[] = "_ZNK8eprosima7fastdds3dds12DDSSQLFilter19DDSFilterExpression8evaluateERKNS_8fastrtps4rtps19SerializedPayload_tERKNS1_14IContentFilter16FilterSampleInfoERKNS5_6GUID_tE";
std::atomic<uint32_t> resolve_status{0};
pthread_mutex_t ring_mutex = PTHREAD_MUTEX_INITIALIZER;
Record ring[128]{};
uint64_t produced = 0, consumed = 0, dropped = 0, pending = 0;
size_t head = 0;

Evaluate original()
{
    static Evaluate fn = []() noexcept -> Evaluate {
        dlerror();
        void* address = dlvsym(RTLD_NEXT, kSymbol, "CODEABI_1.0");
        const char* error = dlerror();
        if (address == nullptr || error != nullptr)
        {
            resolve_status.store(2, std::memory_order_relaxed);
            static const char message[] = "r2f filter trace: RTLD_NEXT evaluate@CODEABI_1.0 unavailable; terminating\n";
            // No substitute bool and no unversioned resolution fallback.
            (void)!write(STDERR_FILENO, message, sizeof(message) - 1);
            _exit(127);
        }
        resolve_status.store(1, std::memory_order_relaxed);
        return reinterpret_cast<Evaluate>(address);
    }();
    return fn;
}

uint64_t now_ns() noexcept
{
    timespec ts{};
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) return 0;
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000ull + static_cast<uint64_t>(ts.tv_nsec);
}
void copy_guid(uint8_t* out, const Guid& guid) noexcept
{
    static_assert(sizeof(guid.guidPrefix.value) == 12, "GUID prefix size");
    static_assert(sizeof(guid.entityId.value) == 4, "GUID entity size");
    std::memcpy(out, guid.guidPrefix.value, 12);
    std::memcpy(out + 12, guid.entityId.value, 4);
}
void append(Record& record) noexcept
{
    // Valid process-private mutex, static storage: no allocating queue and no
    // exception from the observer. The native call has already completed.
    if (pthread_mutex_lock(&ring_mutex) != 0) return;
    record.ordinal = ++produced;
    if (pending == 128) ++dropped;
    else
    {
        ring[(head + pending) % 128] = record;
        ++pending;
    }
    pthread_mutex_unlock(&ring_mutex);
}
} // anonymous namespace

namespace eprosima { namespace fastdds { namespace dds { namespace DDSSQLFilter {
bool DDSFilterExpression::evaluate(
        const IContentFilter::SerializedPayload& payload,
        const IContentFilter::FilterSampleInfo& sample_info,
        const IContentFilter::GUID_t& reader_guid) const
{
    const int entry_errno = errno;
    Evaluate fn = original();
    Record record{};
    record.schema_version = 1;
    record.struct_size = sizeof(Record);
    record.pid = static_cast<uint32_t>(getpid());
    record.payload_length = payload.length;
    record.payload_copied = payload.data == nullptr ? 0 :
            (payload.length < sizeof(record.payload) ? payload.length : sizeof(record.payload));
    record.encapsulation = payload.encapsulation;
    const auto& identity = sample_info.sample_identity;
    record.sequence_high = identity.sequence_number().high;
    record.sequence_low = identity.sequence_number().low;
    copy_guid(record.reader_guid, reader_guid);
    copy_guid(record.writer_guid, identity.writer_guid());
    if (record.payload_copied != 0)
        std::memcpy(record.payload, payload.data, record.payload_copied);
    record.started_ns = now_ns();
    errno = entry_errno;
    try
    {
        const bool result = fn(this, payload, sample_info, reader_guid);
        const int returned_errno = errno;
        record.completed_ns = now_ns();
        record.decision = result ? 1 : 0;
        append(record);
        errno = returned_errno;
        return result;
    }
    catch (...)
    {
        const int thrown_errno = errno;
        record.completed_ns = now_ns();
        record.status = 1;
        record.decision = -1;
        append(record);
        errno = thrown_errno;
        throw;
    }
}
}}}} // namespaces

extern "C" __attribute__((visibility("default"))) int32_t r2f_fastdds_filter_trace_pop_v1(
        void* out, uint32_t out_bytes) noexcept
{
    if (out == nullptr || out_bytes != sizeof(Record)) return -1;
    const int saved_errno = errno;
    if (pthread_mutex_lock(&ring_mutex) != 0) { errno = saved_errno; return 0; }
    int32_t result = 0;
    if (pending != 0)
    {
        std::memcpy(out, &ring[head], sizeof(Record));
        head = (head + 1) % 128;
        --pending;
        ++consumed;
        result = 1;
    }
    pthread_mutex_unlock(&ring_mutex);
    errno = saved_errno;
    return result;
}
extern "C" __attribute__((visibility("default"))) int32_t r2f_fastdds_filter_trace_stats_v1(
        void* out, uint32_t out_bytes) noexcept
{
    if (out == nullptr || out_bytes != sizeof(Stats)) return -1;
    const int saved_errno = errno;
    Stats stats{};
    stats.schema_version = 1;
    stats.struct_size = sizeof(Stats);
    if (pthread_mutex_lock(&ring_mutex) != 0) { errno = saved_errno; return -1; }
    stats.produced = produced;
    stats.consumed = consumed;
    stats.dropped = dropped;
    stats.pending = pending;
    stats.resolve_status = resolve_status.load(std::memory_order_relaxed);
    pthread_mutex_unlock(&ring_mutex);
    std::memcpy(out, &stats, sizeof(stats));
    errno = saved_errno;
    return 0;
}
