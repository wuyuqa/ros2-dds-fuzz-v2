"""Explicit ROS message contracts for the existing topology carrier.

Importing this module never imports ROS. These declarations are input bindings;
only a worker can observe generated schemas, type support and serialized bytes.
"""
from copy import deepcopy


_TYPE_CONTRACTS = {
    "std_msgs/msg/String": {
        "field_types": {"data": "string"},
        "nested_field_types": {},
        "payload_encoding": "utf8_json_string",
    },
    "std_msgs/msg/UInt8MultiArray": {
        "field_types": {"layout": "std_msgs/MultiArrayLayout", "data": "sequence<uint8>"},
        "nested_field_types": {
            "MultiArrayLayout": {
                "dim": "sequence<std_msgs/MultiArrayDimension>", "data_offset": "uint32"},
            "MultiArrayDimension": {"label": "string", "size": "uint32", "stride": "uint32"},
        },
        "payload_encoding": "utf8_json_uint8_sequence",
    },
}
SUPPORTED_MESSAGE_TYPES = tuple(_TYPE_CONTRACTS)


def message_type_contract(type_name):
    """Return an input-only schema; unknown names never resolve arbitrary code."""
    if not isinstance(type_name, str) or type_name not in _TYPE_CONTRACTS:
        raise ValueError("message_type_not_expressible:" + str(type_name))
    return {
        "type_name": type_name,
        **deepcopy(_TYPE_CONTRACTS[type_name]),
        "type_support_status": "unverified",
        "execution_evidence": False,
    }


class MessageTypeBindingError(RuntimeError):
    """A named binding/serialization failure; never a SUT contract verdict."""

    def __init__(self, code, detail):
        self.code = code
        super().__init__(code + ":" + str(detail))


class RosMessageCodec:
    """Bind generated classes and preserve the same envelope in two ROS schemas."""

    def __init__(self, type_name):
        from importlib import import_module
        self.contract = message_type_contract(type_name)
        self.type_name = type_name
        try:
            module = import_module("std_msgs.msg")
            from rclpy.type_support import check_for_type_support
            from rclpy.exceptions import NoTypeSupportImportedException
            from rclpy.serialization import serialize_message, deserialize_message
            self.message_class = getattr(module, type_name.rsplit("/", 1)[-1])
            if self.message_class.__name__ != type_name.rsplit("/", 1)[-1]:
                raise MessageTypeBindingError("message_class_mismatch", self.message_class.__name__)
            schemas = {self.message_class.__name__: self.contract["field_types"],
                       **self.contract["nested_field_types"]}
            observed = {}
            for name, expected in schemas.items():
                cls = getattr(module, name)
                fields = dict(cls.get_fields_and_field_types())
                if fields != expected:
                    raise MessageTypeBindingError("message_schema_mismatch", name)
                try:
                    check_for_type_support(cls)
                except NoTypeSupportImportedException as exc:
                    raise MessageTypeBindingError("message_type_support_unavailable", exc) from exc
                observed[name] = fields
            self.serialize = serialize_message
            self.deserialize = deserialize_message
        except (ImportError, AttributeError, RuntimeError, TypeError) as exc:
            if isinstance(exc, MessageTypeBindingError):
                raise
            raise MessageTypeBindingError("message_type_support_unavailable", exc) from exc
        self.support_receipt = {
            "status": "observed", "type_name": type_name,
            "generated_class": self.message_class.__module__ + "." + self.message_class.__name__,
            "observed_field_types": observed[self.message_class.__name__],
            "observed_nested_field_types": {k: v for k, v in observed.items()
                                           if k != self.message_class.__name__},
            "api": "rclpy.type_support.check_for_type_support",
            "payload_encoding": self.contract["payload_encoding"],
            "candidate_eligible": False,
        }

    def make_message(self, text):
        message = self.message_class()
        message.data = text if self.type_name == "std_msgs/msg/String" else list(text.encode("utf-8"))
        return message

    def payload_text(self, message):
        if type(message) is not self.message_class:
            raise MessageTypeBindingError("message_class_mismatch", type(message).__name__)
        if self.type_name == "std_msgs/msg/String":
            if not isinstance(message.data, str):
                raise MessageTypeBindingError("message_payload_invalid", "String.data is not text")
            return message.data
        if message.layout.dim or message.layout.data_offset != 0:
            raise MessageTypeBindingError("message_payload_invalid", "noncanonical array layout")
        try:
            return bytes(message.data).decode("utf-8")
        except (TypeError, ValueError, UnicodeError) as exc:
            raise MessageTypeBindingError("message_payload_invalid", exc) from exc

    def sample_fields(self, message, *, capture_stage):
        import hashlib
        text = self.payload_text(message)
        try:
            serialized = bytes(self.serialize(message))
            restored = self.deserialize(serialized, self.message_class)
            if self.payload_text(restored) != text:
                raise MessageTypeBindingError("message_serialization_roundtrip_mismatch", self.type_name)
        except (TypeError, ValueError, RuntimeError) as exc:
            if isinstance(exc, MessageTypeBindingError):
                raise
            raise MessageTypeBindingError("message_serialization_failed", exc) from exc
        return {
            "payload_text": text,
            "string_data": text if self.type_name == "std_msgs/msg/String" else None,
            "payload_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
            "payload_encoding": self.contract["payload_encoding"],
            "serialization_receipt": {
                "api": "rclpy.serialization.serialize_message",
                "roundtrip_api": "rclpy.serialization.deserialize_message",
                "capture_stage": capture_stage, "wire_capture": False,
                "roundtrip_equal": True, "serialized_size": len(serialized),
                "serialized_sha256": hashlib.sha256(serialized).hexdigest(),
                "serialized_hex": serialized.hex(),
            },
        }
