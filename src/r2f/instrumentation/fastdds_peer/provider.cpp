// Opt-in read-only source instrumentation for official Fast DDS v2.14.7.
// Build ONLY against the six friend-patched headers and matching vendor build.
// No private-offset casts, listener replacement, match mutation or DDS writes.
//
// C ABI (native endian, fixed-width fields, sizeof(result)==64):
//   int32_t r2f_fastdds_peer_{writer,reader}_v1(
//       void* current_dds_object, uint8_t* guids, uint64_t capacity,
//       r2f_fastdds_peer_result_v1* out, uint32_t out_bytes);
// Input must be the current DataWriter*/DataReader*, NOT Impl*/RTPS*/rmw handle.
// Caller pins endpoint lifetime AND prevents concurrent enable/delete; there is
// no stale endpoint/impl/RTPS pointer cache. Invalid non-null pointers cannot be
// validated by C++; they violate this ABI's precondition.
//
// guids has capacity * 16 writable bytes, may be null only for capacity==0.
// Bytes are guidPrefix.value[12] then entityId.value[4], NOT a C++ struct dump.
// out must be disjoint from guids and exactly out_bytes==64. ABI mismatch does
// not touch out. All other failures set complete=0, copied=0. Short buffer
// reports required_count without touching guids. Retry captures NEW state.
//
// Supported: RELIABLE + StatefulWriter/StatefulReader. Other types/kinds return
// UNSUPPORTED, never successful empty. NOT_ENABLED means no native entity yet.
// Writer membership: union of local/datasharing/remote ReaderProxy GUIDs (no
// alive filter, exactly matched_reader_is_matched). Reader membership: only
// is_alive_ && WriterProxy::is_alive(), exactly matched_writer_is_matched.
// Each snapshot is copied/counted under ONE original RTPS entity mutex. No DDS
// matched-status getter (which could reset deltas) is called. It is NOT an
// atomic cross-entity snapshot or a future/past matching-continuity guarantee.
//
// snapshot_ordinal is provider-instance observation ordering ONLY, NOT
// match_epoch; it advances on supported snapshots including sizing/short-buffer
// observations. It resets when this provider instance is reloaded. Count and
// GUIDs are a set, sorted/deduplicated from a single native traversal; duplicates
// across transport-specific lists, if any, do not change bool membership.

#include <fastdds/dds/publisher/DataWriter.hpp>
#include <fastdds/dds/subscriber/DataReader.hpp>
#include <fastdds/rtps/reader/StatefulReader.h>
#include <fastdds/rtps/writer/ReaderProxy.h>
#include <fastdds/rtps/writer/StatefulWriter.h>
#include <fastdds/publisher/DataWriterImpl.hpp>
#include <fastdds/subscriber/DataReaderImpl.hpp>
#include <rtps/reader/WriterProxy.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <mutex>
#include <vector>

extern "C" {

struct r2f_fastdds_peer_result_v1
{
    uint32_t schema_version;
    uint32_t struct_size;
    int32_t status;
    uint32_t endpoint_kind; // 1 writer, 2 reader
    uint8_t local_guid[16];
    uint64_t required_count;
    uint64_t copied;
    uint64_t snapshot_ordinal;
    uint32_t complete;
    uint32_t reserved; // zero
};

} // extern "C"

static_assert(sizeof(r2f_fastdds_peer_result_v1) == 64, "unsupported result ABI");
static_assert(offsetof(r2f_fastdds_peer_result_v1, schema_version) == 0, "ABI");
static_assert(offsetof(r2f_fastdds_peer_result_v1, struct_size) == 4, "ABI");
static_assert(offsetof(r2f_fastdds_peer_result_v1, status) == 8, "ABI");
static_assert(offsetof(r2f_fastdds_peer_result_v1, endpoint_kind) == 12, "ABI");
static_assert(offsetof(r2f_fastdds_peer_result_v1, local_guid) == 16, "ABI");
static_assert(offsetof(r2f_fastdds_peer_result_v1, required_count) == 32, "ABI");
static_assert(offsetof(r2f_fastdds_peer_result_v1, copied) == 40, "ABI");
static_assert(offsetof(r2f_fastdds_peer_result_v1, snapshot_ordinal) == 48, "ABI");
static_assert(offsetof(r2f_fastdds_peer_result_v1, complete) == 56, "ABI");
static_assert(offsetof(r2f_fastdds_peer_result_v1, reserved) == 60, "ABI");

namespace r2f {
namespace {

namespace rtps = eprosima::fastrtps::rtps;
namespace dds = eprosima::fastdds::dds;
using GuidBytes = std::array<uint8_t, 16>;
using Result = r2f_fastdds_peer_result_v1;

enum Status : int32_t
{
    OK = 0,
    INVALID_ARGUMENT = 1,
    UNSUPPORTED = 2,
    NOT_ENABLED = 3,
    BUFFER_TOO_SMALL = 4,
    INTERNAL_ERROR = 5,
    ABI_MISMATCH = 6
};

std::atomic<uint64_t> observation_ordinal{0};

GuidBytes guid_bytes(const rtps::GUID_t& guid) noexcept
{
    GuidBytes bytes{};
    static_assert(sizeof(guid.guidPrefix.value) == 12, "GUID prefix size");
    static_assert(sizeof(guid.entityId.value) == 4, "entity ID size");
    std::memcpy(bytes.data(), guid.guidPrefix.value, 12);
    std::memcpy(bytes.data() + 12, guid.entityId.value, 4);
    return bytes;
}

int32_t finish(
        std::vector<GuidBytes>& peers,
        uint8_t* buffer,
        uint64_t capacity,
        Result& result)
{
    // Still inside the original entity lock. This produces the full bool-
    // membership set even if a transport list ever contains duplicate GUIDs.
    std::sort(peers.begin(), peers.end());
    peers.erase(std::unique(peers.begin(), peers.end()), peers.end());
    result.required_count = static_cast<uint64_t>(peers.size());
    const uint64_t ordinal = observation_ordinal.fetch_add(1, std::memory_order_relaxed) + 1;
    if (ordinal == 0) // serial exhaustion cannot be mislabeled as a valid receipt
    {
        return INTERNAL_ERROR;
    }
    result.snapshot_ordinal = ordinal;
    if (capacity < result.required_count)
    {
        return BUFFER_TOO_SMALL;
    }
    for (size_t i = 0; i < peers.size(); ++i)
    {
        std::memcpy(buffer + i * 16, peers[i].data(), 16);
    }
    result.copied = result.required_count;
    result.complete = 1;
    return OK;
}

} // anonymous namespace

// This exact class is friended in six source headers. No fields/virtual slots
// are added to ANY vendor class. All traversals below are statically typed.
class FastddsPeerAccess
{
public:

    static int32_t writer(
            dds::DataWriter* endpoint, uint8_t* buffer, uint64_t capacity, Result& result)
    {
        if (!endpoint->impl_ || !endpoint->impl_->writer_)
        {
            return NOT_ENABLED;
        }
        auto* native = dynamic_cast<rtps::StatefulWriter*>(endpoint->impl_->writer_);
        if (!native)
        {
            return UNSUPPORTED;
        }
        std::lock_guard<eprosima::fastrtps::RecursiveTimedMutex> guard(native->getMutex());
        if (native->getAttributes().reliabilityKind != rtps::RELIABLE)
        {
            return UNSUPPORTED;
        }
        const auto local = guid_bytes(native->getGuid());
        std::memcpy(result.local_guid, local.data(), local.size());
        std::vector<GuidBytes> peers;
        // Exactly the three containers used by matched_reader_is_matched.
        // No ReaderProxy alive/reliability filter is part of that predicate.
        for (const auto* proxy : native->matched_local_readers_)
        {
            peers.push_back(guid_bytes(proxy->guid()));
        }
        for (const auto* proxy : native->matched_datasharing_readers_)
        {
            peers.push_back(guid_bytes(proxy->guid()));
        }
        for (const auto* proxy : native->matched_remote_readers_)
        {
            peers.push_back(guid_bytes(proxy->guid()));
        }
        return finish(peers, buffer, capacity, result);
    }

    static int32_t reader(
            dds::DataReader* endpoint, uint8_t* buffer, uint64_t capacity, Result& result)
    {
        if (!endpoint->impl_ || !endpoint->impl_->reader_)
        {
            return NOT_ENABLED;
        }
        auto* native = dynamic_cast<rtps::StatefulReader*>(endpoint->impl_->reader_);
        if (!native)
        {
            return UNSUPPORTED;
        }
        std::lock_guard<eprosima::fastrtps::RecursiveTimedMutex> guard(native->getMutex());
        if (native->getAttributes().reliabilityKind != rtps::RELIABLE)
        {
            return UNSUPPORTED;
        }
        const auto local = guid_bytes(native->getGuid());
        std::memcpy(result.local_guid, local.data(), local.size());
        std::vector<GuidBytes> peers;
        if (native->is_alive_)
        {
            for (const auto* proxy : native->matched_writers_)
            {
                if (proxy->is_alive())
                {
                    peers.push_back(guid_bytes(proxy->guid()));
                }
            }
        }
        return finish(peers, buffer, capacity, result);
    }
};

namespace {

// Failure paths cannot escape the ABI or leave a complete/partially valid
// receipt. This does not and cannot catch invalid-pointer memory faults.
template<typename Endpoint, typename Capture>
int32_t capture(
        void* endpoint, uint8_t* buffer, uint64_t capacity,
        Result* out, uint32_t out_bytes, uint32_t kind, Capture take_snapshot) noexcept
{
    if (!out)
    {
        return INVALID_ARGUMENT;
    }
    if (out_bytes != sizeof(Result))
    {
        return ABI_MISMATCH;
    }
    Result result{};
    result.schema_version = 1;
    result.struct_size = sizeof(Result);
    result.endpoint_kind = kind;
    result.status = INVALID_ARGUMENT;
    if (!endpoint || (capacity && !buffer) ||
            capacity > std::numeric_limits<size_t>::max() / 16)
    {
        *out = result;
        return result.status;
    }
    try
    {
        // void* -> exact documented DDS type, never a private-layout cast.
        result.status = take_snapshot(static_cast<Endpoint*>(endpoint), buffer, capacity, result);
    }
    catch (...)
    {
        result = Result{};
        result.schema_version = 1;
        result.struct_size = sizeof(Result);
        result.endpoint_kind = kind;
        result.status = INTERNAL_ERROR;
    }
    *out = result;
    return result.status;
}

} // anonymous namespace
} // namespace r2f

#if defined(_WIN32)
#define R2F_PEER_EXPORT __declspec(dllexport)
#else
#define R2F_PEER_EXPORT __attribute__((visibility("default")))
#endif

extern "C" R2F_PEER_EXPORT int32_t r2f_fastdds_peer_writer_v1(
        void* current_dds_datawriter, uint8_t* guids, uint64_t capacity,
        r2f_fastdds_peer_result_v1* out, uint32_t out_bytes) noexcept
{
    return r2f::capture<eprosima::fastdds::dds::DataWriter>(
        current_dds_datawriter, guids, capacity, out, out_bytes, 1, r2f::FastddsPeerAccess::writer);
}

extern "C" R2F_PEER_EXPORT int32_t r2f_fastdds_peer_reader_v1(
        void* current_dds_datareader, uint8_t* guids, uint64_t capacity,
        r2f_fastdds_peer_result_v1* out, uint32_t out_bytes) noexcept
{
    return r2f::capture<eprosima::fastdds::dds::DataReader>(
        current_dds_datareader, guids, capacity, out, out_bytes, 2, r2f::FastddsPeerAccess::reader);
}
