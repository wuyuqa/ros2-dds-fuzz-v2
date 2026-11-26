// Optional public-API companion; the matched-peer provider source/ABI is unchanged.
#include <fastdds/dds/publisher/DataWriter.hpp>
#include <fastdds/dds/subscriber/DataReader.hpp>
#include <fastdds/rtps/common/Guid.h>
#include <cstdint>
#include <cstring>
#if defined(_WIN32)
#define R2F_LOCAL_EXPORT __declspec(dllexport)
#else
#define R2F_LOCAL_EXPORT __attribute__((visibility("default")))
#endif

// Local public-API observations, separate from RTPS matched-peer snapshots.
// The caller pins the current endpoint handle and serializes local create/delete.
// Only find_topic's additional reference is owned/deleted here; no borrowed
// endpoint, publisher/subscriber, participant or source topic is destroyed.
#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/publisher/Publisher.hpp>
#include <fastdds/dds/subscriber/Subscriber.hpp>
#include <fastdds/dds/topic/Topic.hpp>
#include <sstream>
#include <string>

namespace {
std::string local_quote(const std::string& value)
{
    static const char hex[] = "0123456789abcdef";
    std::string out = "\"";
    for (unsigned char ch : value)
    {
        if (ch == '"' || ch == '\\') { out += '\\'; out += char(ch); }
        else if (ch < 32) { out += "\\u00"; out += hex[ch >> 4]; out += hex[ch & 15]; }
        else { out += char(ch); }
    }
    return out + '"';
}
std::string local_guid(const eprosima::fastrtps::rtps::GUID_t& guid)
{
    static const char hex[] = "0123456789abcdef";
    std::string out;
    for (int i = 0; i < 16; ++i)
    {
        const auto b = i < 12 ? guid.guidPrefix.value[i] : guid.entityId.value[i - 12];
        out += hex[b >> 4]; out += hex[b & 15];
    }
    return out;
}
uintptr_t local_address(const void* value) { return reinterpret_cast<uintptr_t>(value); }
}

extern "C" R2F_LOCAL_EXPORT int32_t r2f_fastdds_local_v1(void* endpoint, int32_t reader, uint32_t domain,
        const char* query, char* output, uint64_t capacity) noexcept
{
    namespace dds = eprosima::fastdds::dds;
    try
    {
        if (!output || capacity < 2 || domain > 231 || (reader != 0 && reader != 1)) return 1;
        auto roots = dds::DomainParticipantFactory::get_instance()->lookup_participants(domain);
        if (roots.size() > 256) return 2;
        std::ostringstream out;
        out << "{\"schema_version\":1,\"domain_id\":" << domain << ",\"roots\":[";
        bool first = true;
        for (auto* root : roots)
        {
            if (!root || root->get_domain_id() != domain) return 3;
            if (!first) out << ',';
            first = false;
            out << "{\"participant_handle\":" << local_address(root)
                << ",\"participant_gid\":" << local_quote(local_guid(root->guid()))
                << ",\"domain_id\":" << root->get_domain_id() << '}';
        }
        out << ']';
        if (endpoint)
        {
            const dds::DomainParticipant* participant;
            const dds::TopicDescription* source;
            const void* parent;
            std::string gid;
            if (reader)
            {
                auto* object = static_cast<dds::DataReader*>(endpoint);
                auto* subscriber = object->get_subscriber();
                if (!subscriber) return 4;
                parent = subscriber;
                participant = subscriber->get_participant();
                source = object->get_topicdescription();
                gid = local_guid(object->guid());
            }
            else
            {
                auto* object = static_cast<dds::DataWriter*>(endpoint);
                auto* publisher = object->get_publisher();
                if (!publisher) return 4;
                parent = publisher;
                participant = publisher->get_participant();
                source = object->get_topic();
                gid = local_guid(object->guid());
            }
            if (!participant || !parent || !source || participant->get_domain_id() != domain) return 4;
            out << ",\"local_handle\":" << local_address(endpoint)
                << ",\"local_gid\":" << local_quote(gid)
                << ",\"role_kind\":" << local_quote(reader ? "reader" : "writer")
                << ",\"parent_handle\":" << local_address(parent)
                << ",\"participant_handle\":" << local_address(participant)
                << ",\"participant_gid\":" << local_quote(local_guid(participant->guid()))
                << ",\"source_name\":" << local_quote(source->get_name())
                << ",\"source_type_name\":" << local_quote(source->get_type_name());
            if (query)
            {
                if (std::strlen(query) > 256) return 1;
                auto* owner = const_cast<dds::DomainParticipant*>(participant);
                auto* found = owner->find_topic(query, eprosima::fastrtps::Duration_t(0, 0));
                if (found)
                {
                    // Copy before release. Always release even if allocation throws.
                    std::string name, type;
                    uintptr_t found_owner = 0;
                    try { name = found->get_name(); type = found->get_type_name();
                          found_owner = local_address(found->get_participant()); }
                    catch (...) { owner->delete_topic(found); throw; }
                    const int release = owner->delete_topic(found)();
                    out << ",\"lookup_status\":\"found\",\"actual_name\":" << local_quote(name)
                        << ",\"actual_type_name\":" << local_quote(type)
                        << ",\"result_participant_handle\":" << found_owner
                        << ",\"delete_topic_return_code\":" << release;
                }
                else { out << ",\"lookup_status\":\"not_found\",\"delete_topic_return_code\":null"; }
            }
        }
        out << '}';
        const auto value = out.str();
        if (value.size() + 1 > capacity) return 5;
        std::memcpy(output, value.c_str(), value.size() + 1);
        return 0;
    }
    catch (...) { return 6; }
}
