// Explicit NORMAL CONTROL, separate from the passive PDP trace interposer.
// Only public, typed Fast DDS APIs; no private layouts, sleeps or direct PDP calls.
#include <fastdds/dds/publisher/DataWriter.hpp>
#include <fastdds/dds/publisher/Publisher.hpp>
#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/domain/qos/DomainParticipantQos.hpp>
#include <algorithm>
#include <cstdint>
#include <cstring>

struct RefreshResult {
    uint32_t schema_version, struct_size;
    int32_t status, dds_return;
    uint8_t endpoint_guid[16], participant_guid[16];
    uint32_t before_size, after_size;
    uint8_t before[256], after[256];
    uint32_t complete, reserved;
};
static_assert(sizeof(RefreshResult)==576,"refresh ABI");
static void guid_bytes(uint8_t* out,const eprosima::fastrtps::rtps::GUID_t& guid) {
    std::memcpy(out,guid.guidPrefix.value,12);std::memcpy(out+12,guid.entityId.value,4);
}
extern "C" int32_t r2f_fastdds_participant_refresh_writer_v1(
        void* endpoint,const uint8_t* data,uint32_t length,RefreshResult* out,uint32_t size) noexcept {
    if (!out || size!=sizeof(RefreshResult))return -1;
    *out=RefreshResult{};out->schema_version=1;out->struct_size=sizeof(RefreshResult);
    out->status=1;out->dds_return=-1;
    if (!endpoint || !data || length==0 || length>256)return out->status;
    try {
        auto* writer=static_cast<eprosima::fastdds::dds::DataWriter*>(endpoint);
        auto* publisher=writer->get_publisher();
        auto* participant=publisher?publisher->get_participant():nullptr;
        if (!participant)return out->status=2;
        guid_bytes(out->endpoint_guid,writer->guid());guid_bytes(out->participant_guid,participant->guid());
        eprosima::fastdds::dds::DomainParticipantQos qos;
        auto code=participant->get_qos(qos);out->dds_return=code();
        if (!code)return out->status=3;
        const auto& before=qos.user_data();
        if (before.size()>256)return out->status=4;
        out->before_size=static_cast<uint32_t>(before.size());
        std::copy(before.begin(),before.end(),out->before);
        if (before.size()==length && std::equal(before.begin(),before.end(),data))return out->status=5;
        qos.user_data().resize(length);std::copy(data,data+length,qos.user_data().begin());
        code=participant->set_qos(qos);out->dds_return=code();
        if (!code)return out->status=6;
        eprosima::fastdds::dds::DomainParticipantQos after;
        code=participant->get_qos(after);out->dds_return=code();
        if (!code || after.user_data().size()>256)return out->status=7;
        out->after_size=static_cast<uint32_t>(after.user_data().size());
        std::copy(after.user_data().begin(),after.user_data().end(),out->after);
        if (out->after_size!=length || !std::equal(data,data+length,out->after))return out->status=8;
        out->status=0;out->complete=1;return 0;
    } catch (...) {return out->status=9;}
}
