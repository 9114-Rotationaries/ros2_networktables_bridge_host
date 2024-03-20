import rclpy
from rclpy.node import Node
from rclpy.serialization import deserialize_message
from rosidl_runtime_py.utilities import get_message
from rosidl_runtime_py import get_message_interfaces

class GenericMessageSubscriber(Node):
    def __init__(self, topic_name, callback, **kwargs):
        super().__init__('generic_message_subscriber_node')
        # interpreting type at runtime, using placeholder in constructor
        self.subscription = self.create_subscription(
            rclpy.msg.Message,  # placeholder
            topic_name, 
            self.generic_message_callback, 
            10  # todo: place in configuration file?
        )
        self._callback = callback
        self.class_map = {}

    def generic_message_callback(self, data):
        # find message type before deserializing
        msg_type = self.class_map.get(data._type, None)
        if msg_type is None:
            # dynamic loading if our message type isn't in the cache
            msg_type = get_message(data._type)
            self.class_map[data._type] = msg_type
        
        # this part was in the original code
        msg = deserialize_message(data, msg_type)
        self._callback(msg)

    def unregister(self):
        self.subscription.destroy()