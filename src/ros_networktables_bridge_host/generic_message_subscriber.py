import rospy
from ros_conversions import get_msg_class


class GenericMessageSubscriber:
    def __init__(self, topic_name, callback, **kwargs):
        self._binary_sub = rospy.Subscriber(
            topic_name, rospy.AnyMsg, self.generic_message_callback, **kwargs
        )
        self._callback = callback
        self.class_map = {}

    def generic_message_callback(self, data):
        msg_class = get_msg_class(self.class_map, data._connection_header["type"])
        msg = msg_class().deserialize(data._buff)
        self._callback(msg)

    def unregister(self):
        self._binary_sub.unregister()
