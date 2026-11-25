#include <dds/dds.h>
#include <stddef.h>
#include <stdio.h>
int main(void) {
 printf("{\"endpoint_layout\":[%zu,%zu,%zu,%zu,%zu,%zu,%zu],\"entity_size\":%zu,\"guid_size\":%zu,\"instance_handle_size\":%zu}\n",
 sizeof(dds_builtintopic_endpoint_t),offsetof(dds_builtintopic_endpoint_t,key),
 offsetof(dds_builtintopic_endpoint_t,participant_key),offsetof(dds_builtintopic_endpoint_t,participant_instance_handle),
 offsetof(dds_builtintopic_endpoint_t,topic_name),offsetof(dds_builtintopic_endpoint_t,type_name),offsetof(dds_builtintopic_endpoint_t,qos),
 sizeof(dds_entity_t),sizeof(dds_guid_t),sizeof(dds_instance_handle_t));
 return 0;
}
