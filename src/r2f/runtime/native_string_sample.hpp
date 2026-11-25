// Pinned RMW Fast DDS TypeSupport consumes SerializedData, never a ROS object.
// Use its public CDR-buffer mode so no C/C++ ROS message layout is borrowed.
#ifndef R2F_NATIVE_STRING_SAMPLE_HPP
#define R2F_NATIVE_STRING_SAMPLE_HPP
#include <rmw_fastrtps_shared_cpp/TypeSupport.hpp>
#include <string>
namespace r2f {
class NativeStringWrite {
    eprosima::fastcdr::FastBuffer buffer_;
    eprosima::fastcdr::Cdr encoder_;
    rmw_fastrtps_shared_cpp::SerializedData sample_;
public:
    explicit NativeStringWrite(const std::string& text)
        : encoder_(buffer_, eprosima::fastcdr::Cdr::DEFAULT_ENDIAN,
                   eprosima::fastcdr::CdrVersion::XCDRv1),
          sample_{rmw_fastrtps_shared_cpp::FASTRTPS_SERIALIZED_DATA_TYPE_CDR_BUFFER,
                  &encoder_, nullptr} {
        encoder_.set_encoding_flag(eprosima::fastcdr::EncodingAlgorithmFlag::PLAIN_CDR);
        encoder_.serialize_encapsulation();
        encoder_ << text;
    }
    NativeStringWrite(const NativeStringWrite&) = delete;
    NativeStringWrite& operator=(const NativeStringWrite&) = delete;
    void* data() { return &sample_; }
};
class NativeStringRead {
    eprosima::fastcdr::FastBuffer buffer_;
    rmw_fastrtps_shared_cpp::SerializedData sample_{
        rmw_fastrtps_shared_cpp::FASTRTPS_SERIALIZED_DATA_TYPE_CDR_BUFFER, &buffer_, nullptr};
public:
    NativeStringRead() = default;
    NativeStringRead(const NativeStringRead&) = delete;
    NativeStringRead& operator=(const NativeStringRead&) = delete;
    void* data() { return &sample_; }
    std::string text() {
        eprosima::fastcdr::Cdr decoder(buffer_);
        decoder.read_encapsulation();
        std::string result; decoder >> result; return result;
    }
};
}
#endif
