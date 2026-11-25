// Serialization only: no participant, ROS context, network, or SUT execution.
#include "r2f/runtime/native_string_sample.hpp"
#include <stdexcept>
#include <iostream>
class CdrOnlySupport final : public rmw_fastrtps_shared_cpp::TypeSupport {
public:
    size_t getEstimatedSerializedSize(const void*, const void*) const override {
        throw std::runtime_error("unexpected_ROS_message_ABI");
    }
    bool serializeROSmessage(const void*, eprosima::fastcdr::Cdr&, const void*) const override {
        throw std::runtime_error("unexpected_ROS_message_ABI");
    }
    bool deserializeROSmessage(eprosima::fastcdr::Cdr&, void*, const void*) const override {
        throw std::runtime_error("unexpected_ROS_message_ABI");
    }
};
int main() {
    CdrOnlySupport support;
    for (const auto& text : {std::string(), std::string("native-matching-control"),
                            std::string("matching-\xe4\xb8\xad\xe6\x96\x87"),std::string(4096,'x')}) {
        r2f::NativeStringWrite message(text);r2f::NativeStringRead received;
        auto size=support.getSerializedSizeProvider(message.data())();
        eprosima::fastrtps::rtps::SerializedPayload_t payload(size);
        if (!size || !support.serialize(message.data(),&payload)
                || !support.deserialize(&payload,received.data()) || received.text()!=text) return 1;
    }
    std::cout<<"4 native String CDR roundtrips passed; no DDS entities"<<std::endl;
}
