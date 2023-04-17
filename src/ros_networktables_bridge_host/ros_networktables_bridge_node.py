#!/usr/bin/env python
import rospy
from networktables import (
    NetworkTables,
    NetworkTable,
    NetworkTablesInstance,
    NetworkTableEntry,
)
from ros_conversions import (
    ros_msg_to_string_json,
    string_json_to_ros_msg,
    parse_nt_topic,
    convert_to_nt_topic,
    get_msg_class,
)


NotifyFlags = NetworkTablesInstance.NotifyFlags


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


class ROSNetworkTablesBridge:
    def __init__(self):
        rospy.init_node("ros_networktables_bridge")

        is_server = rospy.get_param("~is_server", False)
        address = rospy.get_param("~address", "")
        port = rospy.get_param("~port", 1735)
        self.update_interval = rospy.get_param("~update_interval", 0.02)
        self.queue_size = rospy.get_param("~queue_size", 10)
        ros_to_nt_key = rospy.get_param("~ros_to_nt_table_key", "ros_to_nt")
        nt_to_ros_key = rospy.get_param("~nt_to_ros_table_key", "nt_to_ros")

        if is_server:
            NetworkTables.startServer(listenAddress=address, port=port)
            rospy.loginfo(f"Starting server on port {port}")
            assert NetworkTables.isServer()
        else:
            NetworkTables.startClient((address, port))
            rospy.loginfo(f"Connecting to {address}:{port}")
        NetworkTables.setUpdateRate(self.update_interval)

        self.ros_to_nt_subtable: NetworkTable = NetworkTables.getTable(ros_to_nt_key)
        self.nt_to_ros_subtable: NetworkTable = NetworkTables.getTable(nt_to_ros_key)
        self.publishers = {}
        self.subscribers = {}
        self.pub_listen_keys = set()
        self.sub_listen_keys = set()

    def create_ros_to_nt_subscriber(self, topic_name):
        if topic_name in self.subscribers:
            rospy.logwarn(f"Topic {topic_name} is already registered as a subscriber.")
            return
        subscriber = GenericMessageSubscriber(
            topic_name,
            lambda msg: self.ros_to_nt_callback(msg, topic_name),
            queue_size=self.queue_size,
        )
        self.subscribers[topic_name] = subscriber
        rospy.loginfo(f"Registering new subscriber: {topic_name}")

    def ros_to_nt_callback(self, msg, topic_name: str):
        try:
            raw_msg = ros_msg_to_string_json(msg)
            self.ros_to_nt_subtable.getEntry(convert_to_nt_topic(topic_name)).setString(
                raw_msg
            )
        except BaseException as e:
            rospy.logerr_throttle(
                1.0, f"Exception in ros_to_nt_callback: {e}", exc_info=e
            )

    def create_nt_to_ros_publisher(self, topic_name, ros_msg_type):
        if topic_name in self.publishers:
            rospy.logwarn(f"Topic {topic_name} is already registered as a publisher.")
            return
        pub = rospy.Publisher(topic_name, ros_msg_type, queue_size=self.queue_size)
        self.publishers[topic_name] = pub
        rospy.loginfo(f"Registering new publisher: {topic_name}")

    def nt_to_ros_callback(
        self, entry: NetworkTableEntry, key: str, value: str, isNew: int
    ):
        try:
            assert len(key) > 0
            table_divider = key.find("/", 1)
            topic_name = parse_nt_topic(key[table_divider + 1 :])
            assert len(topic_name) > 0
            if topic_name[0] != "/":
                topic_name = "/" + topic_name

            ros_msg, ros_msg_type = string_json_to_ros_msg(value)
            if ros_msg is None or ros_msg_type is None:
                rospy.logwarn(f"Failed to parse message from {key}: {value}")
                return

            if topic_name not in self.publishers:
                self.create_nt_to_ros_publisher(topic_name, ros_msg_type)
            pub = self.publishers[topic_name]
            pub.publish(ros_msg)
        except BaseException as e:
            rospy.logerr_throttle(
                1.0,
                f"Exception in nt_to_ros_callback: {e}. Received value: {value}",
                exc_info=e,
            )

    def get_new_keys(self, keys: set, table: NetworkTable) -> set:
        nt_keys = set(table.getKeys())
        return keys.symmetric_difference(nt_keys)

    def run(self):
        rate = rospy.Rate(1.0 / self.update_interval)
        while not rospy.is_shutdown():
            rate.sleep()
            new_subscribers = self.get_new_keys(
                set(self.subscribers.keys()), self.ros_to_nt_subtable
            )
            new_publishers = self.get_new_keys(
                set(self.publishers.keys()), self.nt_to_ros_subtable
            )
            for new_pub_key in new_publishers:
                pub_topic = parse_nt_topic(new_pub_key)
                if not pub_topic.startswith("/"):
                    pub_topic = "/" + pub_topic
                if pub_topic in self.pub_listen_keys:
                    continue
                self.pub_listen_keys.add(pub_topic)
                self.nt_to_ros_subtable.getEntry(new_pub_key).addListener(
                    self.nt_to_ros_callback, NotifyFlags.NEW | NotifyFlags.UPDATE
                )
                rospy.loginfo(f"Add entry listener for {new_pub_key}")
            for new_sub_key in new_subscribers:
                sub_topic = parse_nt_topic(new_sub_key)
                if not sub_topic.startswith("/"):
                    sub_topic = "/" + sub_topic
                if sub_topic in self.sub_listen_keys:
                    continue
                self.sub_listen_keys.add(sub_topic)
                self.create_ros_to_nt_subscriber(sub_topic)
                rospy.loginfo(f"Add entry subscriber for {new_sub_key}")


if __name__ == "__main__":
    bridge = ROSNetworkTablesBridge()
    bridge.run()
