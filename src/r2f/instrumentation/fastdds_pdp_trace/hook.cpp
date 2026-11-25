// Instrumentation ONLY: typed, versioned interposition on pinned Fast DDS 2.14.7.
// No private-layout casts, DDS calls, lock manipulation, sleeps, or stdout.
// Records mark actual entries/returns; pointer values are identities, never
// retained/dereferenced after the native call. Fork/signal use is unsupported.
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <fastdds/rtps/builtin/discovery/participant/PDP.h>
#include <fastdds/rtps/builtin/discovery/participant/PDPSimple.h>
#include <fastdds/rtps/builtin/discovery/participant/PDPListener.h>
#include <fastdds/rtps/builtin/data/ParticipantProxyData.h>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <mutex>
#include <pthread.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

struct Record {
    uint32_t schema_version, struct_size;
    uint64_t ordinal, time_ns, call_id, parent_id, object, proxy;
    uint32_t pid, tid, kind, phase;
    int32_t lock_owned, result;
    uint8_t guid[16];
};
struct Stats {
    uint32_t schema_version, struct_size;
    uint64_t produced, consumed, dropped, pending;
    uint32_t resolve_errors, enabled;
};
static_assert(sizeof(Record) == 96, "record ABI");
static_assert(sizeof(Stats) == 48, "stats ABI");
static_assert(offsetof(Record, guid) == 80, "GUID offset");
namespace {
using namespace eprosima::fastrtps::rtps;
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
Record ring[4096]{};
uint64_t produced=0, consumed=0, dropped=0, pending=0, head=0;
std::atomic<uint64_t> calls{0};
std::atomic<uint32_t> resolve_errors{0};
thread_local uint64_t active_call=0;
bool enabled() noexcept {
    static const bool value=[](){const char* v=std::getenv("R2F_FASTDDS_PDP_TRACE_ENABLED"); return v && std::strcmp(v,"1")==0;}();
    return value;
}
template<class Function> Function resolve(const char* symbol) {
    int saved=errno;
    dlerror(); void* ptr=dlvsym(RTLD_NEXT,symbol,"CODEABI_1.0");
    if (!ptr || dlerror()) {
        ++resolve_errors;
        const char msg[]="r2f PDP trace: versioned original unavailable\n";
        (void)!write(2,msg,sizeof(msg)-1); _exit(127);
    }
    errno=saved; return reinterpret_cast<Function>(ptr);
}
void append(Record row) noexcept {
    int saved=errno;
    timespec ts{}; clock_gettime(CLOCK_MONOTONIC,&ts);
    row.time_ns=uint64_t(ts.tv_sec)*1000000000ull+ts.tv_nsec;
    row.pid=getpid(); row.tid=syscall(SYS_gettid);
    pthread_mutex_lock(&mutex);
    row.ordinal=++produced;
    if(pending==4096) ++dropped;
    else {ring[(head+pending)%4096]=row; ++pending;}
    pthread_mutex_unlock(&mutex);
    errno=saved;
}
struct Span {
    Record row{};
    bool capture;
    Span(uint32_t kind, const void* object, const void* proxy, const GUID_t& guid, int owned=-1):capture(enabled()) {
        if(!capture)return;
        row.schema_version=1; row.struct_size=sizeof(row);row.kind=kind;row.phase=1;
        row.call_id=++calls;row.parent_id=active_call;active_call=row.call_id;
        row.object=reinterpret_cast<uintptr_t>(object);row.proxy=reinterpret_cast<uintptr_t>(proxy);
        row.lock_owned=owned; row.result=-2;
        std::memcpy(row.guid,guid.guidPrefix.value,12);std::memcpy(row.guid+12,guid.entityId.value,4);
        append(row);
    }
    ~Span(){if(capture){row.phase=2;append(row);active_call=row.parent_id;}}
};
}
namespace eprosima { namespace fastrtps { namespace rtps {
ParticipantProxyData* PDPSimple::createParticipantProxyData(const ParticipantProxyData& data,const GUID_t& writer) {
    using Fn=ParticipantProxyData*(*)(PDPSimple*,const ParticipantProxyData&,const GUID_t&);
    static Fn fn=resolve<Fn>("_ZN8eprosima8fastrtps4rtps9PDPSimple26createParticipantProxyDataERKNS1_20ParticipantProxyDataERKNS1_6GUID_tE");
    Span span(3,this,nullptr,data.m_guid);
    auto* result=fn(this,data,writer);span.row.proxy=reinterpret_cast<uintptr_t>(result);span.row.result=result?1:0;return result;
}
void PDPSimple::assignRemoteEndpoints(ParticipantProxyData* data) {
    using Fn=void(*)(PDPSimple*,ParticipantProxyData*);
    static Fn fn=resolve<Fn>("_ZN8eprosima8fastrtps4rtps9PDPSimple21assignRemoteEndpointsEPNS1_20ParticipantProxyDataE");
    Span span(4,this,data,data->m_guid);fn(this,data);span.row.result=1;
}
void PDPListener::process_alive_data(ParticipantProxyData* old_data,ParticipantProxyData& new_data,GUID_t& writer,
        RTPSReader* reader,std::unique_lock<std::recursive_mutex>& lock) {
    using Fn=void(*)(PDPListener*,ParticipantProxyData*,ParticipantProxyData&,GUID_t&,RTPSReader*,std::unique_lock<std::recursive_mutex>&);
    static Fn fn=resolve<Fn>("_ZN8eprosima8fastrtps4rtps11PDPListener18process_alive_dataEPNS1_20ParticipantProxyDataERS3_RNS1_6GUID_tEPNS1_10RTPSReaderERSt11unique_lockISt15recursive_mutexE");
    Span span(old_data?2:1,this,old_data,new_data.m_guid,lock.owns_lock()?1:0);
    fn(this,old_data,new_data,writer,reader,lock);span.row.lock_owned=lock.owns_lock()?1:0;span.row.result=1;
}
bool PDP::remove_remote_participant(const GUID_t& guid,ParticipantDiscoveryInfo::DISCOVERY_STATUS reason) {
    using Fn=bool(*)(PDP*,const GUID_t&,ParticipantDiscoveryInfo::DISCOVERY_STATUS);
    static Fn fn=resolve<Fn>("_ZN8eprosima8fastrtps4rtps3PDP25remove_remote_participantERKNS1_6GUID_tENS1_24ParticipantDiscoveryInfo16DISCOVERY_STATUSE");
    Span span(5,this,nullptr,guid);bool result=fn(this,guid,reason);span.row.result=result?1:0;return result;
}
bool ParticipantProxyData::updateData(ParticipantProxyData& data) {
    using Fn=bool(*)(ParticipantProxyData*,ParticipantProxyData&);
    static Fn fn=resolve<Fn>("_ZN8eprosima8fastrtps4rtps20ParticipantProxyData10updateDataERS2_");
    Span span(7,this,this,m_guid);bool result=fn(this,data);span.row.result=result?1:0;return result;
}
void ParticipantProxyData::clear() {
    using Fn=void(*)(ParticipantProxyData*);
    static Fn fn=resolve<Fn>("_ZN8eprosima8fastrtps4rtps20ParticipantProxyData5clearEv");
    Span span(6,this,this,m_guid);fn(this);span.row.result=1;
}
}}}
#define EXPORT __attribute__((visibility("default")))
// A constructor cannot be wrapped by defining another C++ constructor: that
// would initialize its members twice. Typed free-function ABI aliases enter
// the ORIGINAL complete/base constructor exactly once, without touching this
// before construction. Source is already alive and protected by its caller.
using Proxy=eprosima::fastrtps::rtps::ParticipantProxyData;
extern "C" EXPORT void copy_complete(Proxy*,const Proxy&) asm("_ZN8eprosima8fastrtps4rtps20ParticipantProxyDataC1ERKS2_");
extern "C" EXPORT void copy_complete(Proxy* self,const Proxy& source) {
    using Fn=void(*)(Proxy*,const Proxy&);
    static Fn fn=resolve<Fn>("_ZN8eprosima8fastrtps4rtps20ParticipantProxyDataC1ERKS2_");
    Span span(8,self,&source,source.m_guid);fn(self,source);span.row.result=1;
}
extern "C" EXPORT void copy_base(Proxy*,const Proxy&) asm("_ZN8eprosima8fastrtps4rtps20ParticipantProxyDataC2ERKS2_");
extern "C" EXPORT void copy_base(Proxy* self,const Proxy& source) {
    using Fn=void(*)(Proxy*,const Proxy&);
    static Fn fn=resolve<Fn>("_ZN8eprosima8fastrtps4rtps20ParticipantProxyDataC2ERKS2_");
    Span span(8,self,&source,source.m_guid);fn(self,source);span.row.result=1;
}
extern "C" EXPORT int32_t r2f_fastdds_pdp_trace_pop_v1(Record* out,uint32_t size) noexcept {
    if(!out || size!=sizeof(Record))return -1;
    pthread_mutex_lock(&mutex);
    if(!pending){pthread_mutex_unlock(&mutex);return 0;}
    *out=ring[head];head=(head+1)%4096;--pending;++consumed;
    pthread_mutex_unlock(&mutex);return 1;
}
extern "C" EXPORT int32_t r2f_fastdds_pdp_trace_stats_v1(Stats* out,uint32_t size) noexcept {
    if(!out || size!=sizeof(Stats))return -1;
    pthread_mutex_lock(&mutex);
    *out=Stats{1,sizeof(Stats),produced,consumed,dropped,pending,resolve_errors.load(),enabled()?1u:0u};
    pthread_mutex_unlock(&mutex);return 0;
}
