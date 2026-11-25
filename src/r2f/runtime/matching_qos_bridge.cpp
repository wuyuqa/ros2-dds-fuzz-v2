// Borrow existing endpoints through public, installed RCL/RMW/Fast DDS APIs.
// This adapter changes no SUT binary, installs no hook and creates no endpoint.
#include <rcl/publisher.h>
#include <rcl/subscription.h>
#include <rmw_fastrtps_cpp/get_publisher.hpp>
#include <rmw_fastrtps_cpp/get_subscriber.hpp>
#include <fastdds/dds/publisher/Publisher.hpp>
#include <fastdds/dds/subscriber/Subscriber.hpp>
#include <sstream>
#include <string>
#include <vector>
#include <cstring>

namespace {
std::string json_quote(const std::string& s) {
    static const char hex[] = "0123456789abcdef";
    std::string out = "\"";
    for (unsigned char ch : s) {
        if (ch == '"' || ch == '\\') { out += '\\'; out += char(ch); }
        else if (ch < 32) { out += "\\u00"; out += hex[ch >> 4]; out += hex[ch & 15]; }
        else out += char(ch);
    }
    return out + '"';
}
std::string names_json(const std::vector<std::string>& names) {
    std::string out = "[";
    for (size_t i = 0; i < names.size(); ++i) { if (i) out += ','; out += json_quote(names[i]); }
    return out + ']';
}
template<class Endpoint, class Parent, class Qos>
std::string update(Endpoint* endpoint, const Parent* borrowed, bool apply,
                   std::vector<std::string>& names) {
    if (!endpoint || !borrowed) throw std::runtime_error("native_endpoint_unavailable");
    Qos before, after;
    if (borrowed->get_qos(before) != eprosima::fastrtps::types::ReturnCode_t::RETCODE_OK)
        throw std::runtime_error("native_partition_get_before_failed");
    int code = 0;
    if (apply) {
        Qos wanted = before;
        wanted.partition().names(names);
        code = const_cast<Parent*>(borrowed)->set_qos(wanted)();
    }
    if (borrowed->get_qos(after) != eprosima::fastrtps::types::ReturnCode_t::RETCODE_OK)
        throw std::runtime_error("native_partition_get_after_failed");
    const auto& guid = endpoint->guid();
    const auto* prefix = guid.guidPrefix.value;
    const auto* entity = guid.entityId.value;
    static const char hex[] = "0123456789abcdef";
    std::string gid;
    for (int i=0; i<16; ++i) {
        unsigned char ch = i<12 ? prefix[i] : entity[i-12];
        gid += hex[ch >> 4]; gid += hex[ch & 15];
    }
    return "{\"return_code\":" + std::to_string(code) + ",\"endpoint_gid\":" + json_quote(gid)
        + ",\"before\":" + names_json(before.partition().names())
        + ",\"after\":" + names_json(after.partition().names())
        + ",\"ownership\":" + json_quote(endpoint->get_qos().ownership().kind ==
            eprosima::fastdds::dds::EXCLUSIVE_OWNERSHIP_QOS ? "exclusive" : "shared") + "}";
}
}

extern "C" int r2f_matching_partitions(void* rcl_endpoint, int reader, int apply,
    size_t count, const char* const* names, char* output, size_t capacity) noexcept {
    try {
        if (!rcl_endpoint || !output || count > 64 || (count && !names)) return 1;
        std::vector<std::string> parts;
        for (size_t i=0; i<count; ++i) { if (!names[i]) return 1; parts.emplace_back(names[i]); }
        std::string value;
        if (reader) {
            auto rmw = rcl_subscription_get_rmw_handle(static_cast<rcl_subscription_t*>(rcl_endpoint));
            auto endpoint = rmw_fastrtps_cpp::get_datareader(rmw);
            if (!endpoint) return 2;
            value = update<eprosima::fastdds::dds::DataReader,
                eprosima::fastdds::dds::Subscriber, eprosima::fastdds::dds::SubscriberQos>(
                    endpoint, endpoint->get_subscriber(), apply, parts);
        } else {
            auto rmw = rcl_publisher_get_rmw_handle(static_cast<rcl_publisher_t*>(rcl_endpoint));
            auto endpoint = rmw_fastrtps_cpp::get_datawriter(rmw);
            if (!endpoint) return 2;
            value = update<eprosima::fastdds::dds::DataWriter,
                eprosima::fastdds::dds::Publisher, eprosima::fastdds::dds::PublisherQos>(
                    endpoint, endpoint->get_publisher(), apply, parts);
        }
        if (value.size() + 1 > capacity) return 3;
        std::memcpy(output, value.c_str(), value.size() + 1);
        return 0;
    } catch (...) { return 4; }
}

// Separate native DDS control. These entities are never reported as RCL entities.
#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/publisher/DataWriter.hpp>
#include <fastdds/dds/subscriber/DataReader.hpp>
#include <fastdds/dds/topic/TypeSupport.hpp>
#include <fastdds/dds/core/status/PublicationMatchedStatus.hpp>
#include <fastdds/dds/core/status/SubscriptionMatchedStatus.hpp>
#include "native_string_sample.hpp"
#include <chrono>
#include <thread>
#include <iomanip>
#include <functional>
#include <algorithm>
#include <cstddef>

struct R2fNativeQos {
    int32_t reliability, durability, liveliness, ownership, history, depth;
    int64_t deadline, lease;
    uint32_t partition_count;
    const char* const* partitions;
};
extern "C" size_t r2f_native_matching_qos_layout(int field) noexcept {
    const size_t values[]={sizeof(R2fNativeQos),offsetof(R2fNativeQos,reliability),offsetof(R2fNativeQos,durability),
        offsetof(R2fNativeQos,liveliness),offsetof(R2fNativeQos,ownership),offsetof(R2fNativeQos,history),
        offsetof(R2fNativeQos,depth),offsetof(R2fNativeQos,deadline),offsetof(R2fNativeQos,lease),
        offsetof(R2fNativeQos,partition_count),offsetof(R2fNativeQos,partitions)};
    return field>=0 && field<11 ? values[field] : SIZE_MAX;
}
namespace {
using namespace eprosima::fastdds::dds;
using Return = eprosima::fastrtps::types::ReturnCode_t;
const int64_t infinite_ns = INT64_MAX;
eprosima::fastrtps::Duration_t duration(int64_t ns) {
    if (ns == infinite_ns) return eprosima::fastrtps::c_TimeInfinite;
    return {int32_t(ns / 1000000000), uint32_t(ns % 1000000000)};
}
template<class Qos> void native_qos(Qos& qos, const R2fNativeQos& q) {
    qos.reliability().kind = static_cast<ReliabilityQosPolicyKind>(q.reliability + 1);
    qos.durability().kind = static_cast<DurabilityQosPolicyKind>(q.durability);
    qos.liveliness().kind = static_cast<LivelinessQosPolicyKind>(q.liveliness);
    qos.liveliness().lease_duration = duration(q.lease);
    if (q.lease != infinite_ns) qos.liveliness().announcement_period = duration(q.lease / 3);
    qos.ownership().kind = static_cast<OwnershipQosPolicyKind>(q.ownership);
    qos.history().kind = static_cast<HistoryQosPolicyKind>(q.history);
    qos.history().depth = q.depth;
    qos.deadline().period = duration(q.deadline);
}
std::vector<std::string> partition_vector(const R2fNativeQos& q) {
    std::vector<std::string> names;
    for (uint32_t i=0; i<q.partition_count; ++i) names.emplace_back(q.partitions[i]);
    return names;
}
std::string guid_text(const eprosima::fastrtps::rtps::GUID_t& guid) {
    static const char hex[] = "0123456789abcdef";
    std::string out;
    for (int i=0; i<16; ++i) { unsigned char ch = i<12 ? guid.guidPrefix.value[i] : guid.entityId.value[i-12];
        out += hex[ch>>4]; out += hex[ch&15]; }
    return out;
}
int64_t native_now() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}
std::string seconds_json(const eprosima::fastrtps::Duration_t& d) {
    if (d == eprosima::fastrtps::c_TimeInfinite) return "\"infinite\"";
    std::ostringstream out; out << std::setprecision(17) << (double(d.seconds) + double(d.nanosec)/1e9); return out.str();
}
std::string persistence_guid_string(const eprosima::fastrtps::rtps::GUID_t& guid) {
    auto compact=guid_text(guid); std::string text;
    for(size_t i=0;i<32;i+=2) {
        if(i)text+=(i==24?"|":".");text+=compact.substr(i,2);
    }
    return text;
}
template<class Qos> void persistence_qos(Qos& q, DomainParticipant* pp, bool writer,
        const char* database) {
    if(q.durability().kind!=TRANSIENT_DURABILITY_QOS) return;
    if(!database || database[0]!='/') throw std::runtime_error("native_control_persistence_database_required");
    auto guid=pp->guid(); guid.entityId.value[3]=writer?0x02:0x07;
    auto text=persistence_guid_string(guid);
    auto& props=q.properties().properties();
    props.emplace_back("dds.persistence.plugin","builtin.SQLITE3");
    props.emplace_back("dds.persistence.sqlite3.filename",database);
    props.emplace_back("dds.persistence.guid",text);
}
template<class Qos> std::string persistence_json(const Qos& q) {
    std::string out="{"; bool first=true;
    for(const auto& property:q.properties().properties()) {
        if(property.name().compare(0,16,"dds.persistence.")!=0) continue;
        if(!first)out+=","; first=false;
        out+=json_quote(property.name())+":"+json_quote(property.value());
    }
    return out+"}";
}
template<class Endpoint> std::string native_entity(Endpoint* ep, DomainParticipant* pp,
        const std::string& role, const std::string& topic, const std::string& type,
        const std::vector<std::string>& names) {
    const auto& q=ep->get_qos();
    const char* reliabilities[]={"unused", "best_effort", "reliable"};
    const char* durabilities[]={"volatile", "transient_local", "transient", "persistent"};
    const char* liveliness[]={"automatic", "manual_by_participant", "manual_by_topic"};
    return "{\"role\":"+json_quote(role)+",\"endpoint_gid\":"+json_quote(guid_text(ep->guid()))
      +",\"participant_gid\":"+json_quote(guid_text(pp->guid()))+",\"domain_id\":"+std::to_string(pp->get_domain_id())+",\"observed_monotonic_ns\":"+std::to_string(native_now())+",\"topic\":"+json_quote(topic)
      +",\"type_name\":"+json_quote(type)+",\"persistence\":"+persistence_json(q)+",\"qos\":{\"reliability\":"+json_quote(reliabilities[q.reliability().kind])
      +",\"durability\":"+json_quote(durabilities[q.durability().kind])+",\"liveliness\":"+json_quote(liveliness[q.liveliness().kind])
      +",\"ownership\":"+json_quote(q.ownership().kind == EXCLUSIVE_OWNERSHIP_QOS ? "exclusive":"shared")
      +",\"history\":"+json_quote(q.history().kind == KEEP_ALL_HISTORY_QOS ? "keep_all":"keep_last")
      +",\"depth\":"+std::to_string(q.history().depth)+",\"deadline_sec\":"+seconds_json(q.deadline().period)
      +",\"lease_duration_sec\":"+seconds_json(q.liveliness().lease_duration)+",\"partitions\":"+names_json(names)+"}}";
}
}
// Build qualification only: no participant/context/entity is created here.
extern "C" int r2f_matching_cpp_abi_probe(char* output, size_t capacity) noexcept {
    try {
        eprosima::fastrtps::rtps::GUID_t guid;
        for(int i=0;i<12;++i)guid.guidPrefix.value[i]=static_cast<unsigned char>(i+1);
        guid.entityId.value[0]=0;guid.entityId.value[1]=0;
        guid.entityId.value[2]=1;guid.entityId.value[3]=2;
        std::ostringstream streamed; streamed<<guid;
        std::string result="{\"guid\":"+json_quote(persistence_guid_string(guid))
            +",\"stream_guid\":"+json_quote(streamed.str())
            +",\"finite_duration_sec\":"+seconds_json({1,250000000})+"}";
        if(!output || result.size()+1>capacity)return 1;
        std::memcpy(output,result.c_str(),result.size()+1);return 0;
    }catch(...){return 2;}
}
extern "C" int r2f_native_matching(void* rcl_endpoint, int reader_role, uint32_t domain,
    const char* topic_name, int same_participant, const R2fNativeQos* wq, const R2fNativeQos* rq,
    const char* persistence_database, double timeout_sec, char* output, size_t capacity) noexcept {
    std::vector<std::pair<std::string,std::function<int()>>> cleanup;
    std::string status="unknown", reason, entities="[]", fresh_entities="[]", samples="[]", delivery="null", cleanup_json="[";
    bool clean=true;
    auto inventory=[domain](){std::vector<std::string> result;
        for(auto* pp:DomainParticipantFactory::get_instance()->lookup_participants(domain)) result.push_back(guid_text(pp->guid()));
        std::sort(result.begin(),result.end());return result;};
    auto before_inventory=inventory();
    try {
        if (!rcl_endpoint || !topic_name || !wq || !rq || !output || wq->partition_count>64 || rq->partition_count>64)
            return 1;
        DomainParticipant* original=nullptr;
        std::string type_name;
        if (reader_role) {
            auto ep=rmw_fastrtps_cpp::get_datareader(rcl_subscription_get_rmw_handle(static_cast<rcl_subscription_t*>(rcl_endpoint)));
            if (!ep) throw std::runtime_error("native_control_borrowed_reader_missing");
            original=const_cast<DomainParticipant*>(ep->get_subscriber()->get_participant()); type_name=ep->get_topicdescription()->get_type_name();
        } else {
            auto ep=rmw_fastrtps_cpp::get_datawriter(rcl_publisher_get_rmw_handle(static_cast<rcl_publisher_t*>(rcl_endpoint)));
            if (!ep) throw std::runtime_error("native_control_borrowed_writer_missing");
            original=const_cast<DomainParticipant*>(ep->get_publisher()->get_participant()); type_name=ep->get_topic()->get_type_name();
        }
        if(type_name!="std_msgs::msg::dds_::String_") {
            status="unsupported"; throw std::runtime_error("native_control_requires_String_harness");
        }
        TypeSupport type=original->find_type(type_name);
        if (!type) throw std::runtime_error("native_control_type_support_missing");
        auto* factory=DomainParticipantFactory::get_instance();
        auto participant=[&](){ auto* pp=factory->create_participant(domain, PARTICIPANT_QOS_DEFAULT);
            if (!pp) throw std::runtime_error("native_control_participant_create_failed");
            cleanup.push_back({"participant",[factory,pp](){return factory->delete_participant(pp)();}});
            if(type.register_type(pp)!=Return::RETCODE_OK) throw std::runtime_error("native_control_register_type_failed"); return pp; };
        auto* wp=participant(); auto* rp=same_participant ? wp : participant();
        auto topic=[&](DomainParticipant* pp){auto* t=pp->create_topic(topic_name,type_name,TOPIC_QOS_DEFAULT);
            if(!t) throw std::runtime_error("native_control_topic_create_failed");
            cleanup.push_back({"topic",[pp,t](){return pp->delete_topic(t)();}}); return t;};
        auto* wt=topic(wp); auto* rt=wp==rp? wt:topic(rp);
        auto wparts=partition_vector(*wq); PublisherQos pq; pq.partition().names(wparts);
        auto* pub=wp->create_publisher(pq); if(!pub) throw std::runtime_error("native_control_publisher_create_failed");
        cleanup.push_back({"publisher",[wp,pub](){return wp->delete_publisher(pub)();}});
        auto rparts=partition_vector(*rq); SubscriberQos sq; sq.partition().names(rparts);
        auto* sub=rp->create_subscriber(sq); if(!sub) throw std::runtime_error("native_control_subscriber_create_failed");
        cleanup.push_back({"subscriber",[rp,sub](){return rp->delete_subscriber(sub)();}});
        DataWriterQos w; native_qos(w,*wq); DataReaderQos r; native_qos(r,*rq);
        persistence_qos(w,wp,true,persistence_database); persistence_qos(r,rp,false,persistence_database);
        auto* writer=pub->create_datawriter(wt,w);
        if(!writer) { status=(wq->durability==3?"unsupported":"unknown"); throw std::runtime_error("native_control_create_datawriter_returned_null"); }
        cleanup.push_back({"writer",[pub,writer](){return pub->delete_datawriter(writer)();}});
        auto* reader=sub->create_datareader(rt,r);
        if(!reader) { status=(rq->durability==3?"unsupported":"unknown"); throw std::runtime_error("native_control_create_datareader_returned_null"); }
        cleanup.push_back({"reader",[sub,reader](){return sub->delete_datareader(reader)();}});
        PublisherQos actual_p; SubscriberQos actual_s;
        if(pub->get_qos(actual_p)!=Return::RETCODE_OK || sub->get_qos(actual_s)!=Return::RETCODE_OK)
            throw std::runtime_error("native_control_parent_readback_failed");
        entities="["+native_entity(writer,wp,"writer",wt->get_name(),wt->get_type_name(),actual_p.partition().names())
             +","+native_entity(reader,rp,"reader",rt->get_name(),rt->get_type_name(),actual_s.partition().names())+"]";
        auto until=std::chrono::steady_clock::now()+std::chrono::duration<double>(timeout_sec);
        std::string sample_body; bool first=true; PublicationMatchedStatus ws; SubscriptionMatchedStatus rs;
        do {
            if(writer->get_publication_matched_status(ws)!=Return::RETCODE_OK || reader->get_subscription_matched_status(rs)!=Return::RETCODE_OK)
                throw std::runtime_error("native_control_match_status_read_failed");
            if(!first) sample_body+=","; first=false;
            sample_body+="{\"writer_count\":"+std::to_string(ws.current_count)+",\"reader_count\":"+std::to_string(rs.current_count)
                +",\"writer_total_count\":"+std::to_string(ws.total_count)+",\"reader_total_count\":"+std::to_string(rs.total_count)
                +",\"writer_last_peer_gid\":"+json_quote(guid_text(eprosima::fastrtps::rtps::iHandle2GUID(ws.last_subscription_handle)))
                +",\"reader_last_peer_gid\":"+json_quote(guid_text(eprosima::fastrtps::rtps::iHandle2GUID(rs.last_publication_handle)))
                +",\"monotonic_ns\":"+std::to_string(native_now())+"}";
            samples="["+sample_body+"]";
            if(ws.current_count==1 && rs.current_count==1) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        } while(std::chrono::steady_clock::now()<until);
        if(ws.current_count==1 && rs.current_count==1) {
            const std::string sent="native-matching-control"; r2f::NativeStringWrite message(sent);
            int code=writer->write(message.data(), HANDLE_NIL)();
            delivery="{\"write_return_code\":"+std::to_string(code)+",\"sent\":"+json_quote(sent)+",\"received\":null}";
            until=std::chrono::steady_clock::now()+std::chrono::duration<double>(timeout_sec);
            do { SampleInfo info; r2f::NativeStringRead received;
                auto rc=reader->take_next_sample(received.data(),&info);
                if(rc==Return::RETCODE_OK && info.valid_data) {
                    delivery="{\"write_return_code\":"+std::to_string(code)+",\"sent\":"+json_quote(sent)
                      +",\"received\":"+json_quote(received.text())+",\"sample_identity_writer_guid\":"
                      +json_quote(guid_text(info.sample_identity.writer_guid()))+"}"; break; }
                if(rc!=Return::RETCODE_OK && rc!=Return::RETCODE_NO_DATA) throw std::runtime_error("native_control_take_failed");
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            } while(std::chrono::steady_clock::now()<until);
        }
        if(pub->get_qos(actual_p)!=Return::RETCODE_OK || sub->get_qos(actual_s)!=Return::RETCODE_OK)
            throw std::runtime_error("native_control_fresh_parent_readback_failed");
        fresh_entities="["+native_entity(writer,wp,"writer",wt->get_name(),wt->get_type_name(),actual_p.partition().names())
             +","+native_entity(reader,rp,"reader",rt->get_name(),rt->get_type_name(),actual_s.partition().names())+"]";
        status="observed";
    } catch(const std::exception& e) {reason=e.what();} catch(...) {reason="native_control_unknown_exception";}
    bool first=true;
    for(auto it=cleanup.rbegin();it!=cleanup.rend();++it) {
        int rc=it->second(); clean=clean && rc==0;
        if(!first)cleanup_json+=","; first=false;
        cleanup_json+="{\"kind\":"+json_quote(it->first)+",\"return_code\":"+std::to_string(rc)+"}";
    }
    cleanup_json+="]";
    auto after_inventory=inventory(); clean=clean && before_inventory==after_inventory;
    std::string result="{\"status\":"+json_quote(status)+",\"reason\":"+json_quote(reason)+",\"entities\":"+entities
        +",\"participant_inventory_before\":"+names_json(before_inventory)+",\"participant_inventory_after\":"+names_json(after_inventory)+",\"fresh_entities\":"+fresh_entities+",\"samples\":"+samples+",\"delivery\":"+delivery+",\"cleanup\":"+cleanup_json
        +",\"cleanup_confirmed\":"+(clean?"true":"false")+"}";
    if(result.size()+1>capacity)return 2;
    std::memcpy(output,result.c_str(),result.size()+1); return 0;
}
