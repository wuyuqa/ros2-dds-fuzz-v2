"""Generated-message and serialization ABI doubles; never ROS/CDR execution.

Schemas are authored from Jazzy generated definitions. Fixture bytes have an
explicit non-CDR prefix and must never qualify real DDS serialization evidence.
"""
import json
from types import ModuleType, SimpleNamespace


def add_generated_message_fixtures(modules):
    for name in ('rclpy.type_support', 'rclpy.serialization', 'rclpy.exceptions'):
        modules[name] = ModuleType(name)
    schemas = {
        'String': {'data': 'string'},
        'UInt8MultiArray': {'layout': 'std_msgs/MultiArrayLayout', 'data': 'sequence<uint8>'},
        'MultiArrayLayout': {'dim': 'sequence<std_msgs/MultiArrayDimension>', 'data_offset': 'uint32'},
        'MultiArrayDimension': {'label': 'string', 'size': 'uint32', 'stride': 'uint32'},
    }
    def message_init(self):
        self.data = '' if type(self).__name__ == 'String' else []
        self.layout = SimpleNamespace(dim=[], data_offset=0)
    for name, fields in schemas.items():
        cls = type(name, (), {'__init__': message_init,
                             'get_fields_and_field_types': classmethod(lambda cls, f=fields: dict(f))})
        setattr(modules['std_msgs.msg'], name, cls)
    # Same inheritance and constructor as the public Jazzy exception.
    class NoTypeSupportImportedException(Exception):
        def __init__(self):
            super().__init__('no type_support imported')
    modules['rclpy.exceptions'].NoTypeSupportImportedException = NoTypeSupportImportedException
    modules['rclpy.type_support'].check_for_type_support = lambda cls: None
    def serialize(message):
        return b'fixture:' + json.dumps(message.data, ensure_ascii=False).encode('utf-8')
    def deserialize(data, cls):
        message = cls()
        message.data = json.loads(data[len(b'fixture:'):].decode('utf-8'))
        return message
    modules['rclpy.serialization'].serialize_message = serialize
    modules['rclpy.serialization'].deserialize_message = deserialize


def bound_message_codec(type_name='std_msgs/msg/String'):
    """Use the production codec with only the external ROS SDK substituted."""
    import sys
    from unittest.mock import patch
    from r2f.runtime.topology_message_types import RosMessageCodec
    modules = {name: ModuleType(name) for name in ('rclpy', 'std_msgs', 'std_msgs.msg')}
    add_generated_message_fixtures(modules)
    with patch.dict(sys.modules, modules):
        return RosMessageCodec(type_name)
