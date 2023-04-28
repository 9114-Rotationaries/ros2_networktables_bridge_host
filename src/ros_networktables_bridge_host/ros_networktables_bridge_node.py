#!/usr/bin/env python
from typing import Dict, Set
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
)
from generic_message_subscriber import GenericMessageSubscriber


NotifyFlags = NetworkTablesInstance.NotifyFlags


class ROSNetworkTablesBridge:
    """
    This class is a bridge between ROS (Robot Operating System) and NetworkTables,
    allowing messages from ROS topics to be forwarded to NetworkTables and vice versa.
    The bridge can operate as either a NetworkTables server or a client,
    connecting to a remote server.
    """

    def __init__(self):
        # Initialize the ROS node
        rospy.init_node("ros_networktables_bridge")

        # Get the ROS parameters
        is_server = bool(rospy.get_param("~is_server", False))
        address = str(rospy.get_param("~address", "")).strip()
        port = int(rospy.get_param("~port", 1735))
        self.update_interval = float(rospy.get_param("~update_interval", 0.02))
        self.queue_size = int(rospy.get_param("~queue_size", 10))
        ros_to_nt_key = str(rospy.get_param("~ros_to_nt_table_key", "ros_to_nt"))
        nt_to_ros_key = str(rospy.get_param("~nt_to_ros_table_key", "nt_to_ros"))
        topics_request_key = str(rospy.get_param("~topics_request_key", "@topics"))

        # Start NetworkTables server or client based on the is_server parameter
        if is_server:
            NetworkTables.startServer(listenAddress=address, port=port)
            rospy.loginfo(f"Starting server on port {port}")
            if len(address) > 0:
                rospy.loginfo(f"Address is {address}")
            assert NetworkTables.isServer()
        else:
            NetworkTables.startClient((address, port))
            rospy.loginfo(f"Connecting to {address}:{port}")
        NetworkTables.setUpdateRate(self.update_interval)

        # Get NetworkTables subtables
        self.ros_to_nt_subtable: NetworkTable = NetworkTables.getTable(ros_to_nt_key)
        self.nt_to_ros_subtable: NetworkTable = NetworkTables.getTable(nt_to_ros_key)
        self.ros_to_nt_topic_requests_entry: NetworkTableEntry = (
            self.ros_to_nt_subtable.getEntry(topics_request_key)
        )
        self.nt_to_ros_topic_requests_entry: NetworkTableEntry = (
            self.nt_to_ros_subtable.getEntry(topics_request_key)
        )

        # Initialize dictionaries and sets for publishers, subscribers, and keys
        self.publishers: Dict[str, rospy.Publisher] = {}
        self.pub_listen_keys: Set[str] = set()
        self.pub_listen_handles: Dict[str, int] = {}

        self.subscribers: Dict[str, GenericMessageSubscriber] = {}
        self.sub_listen_keys: Set[str] = set()

        # Get the ROS namespace
        self.namespace: str = rospy.get_namespace()
        rospy.loginfo(f"Bridge namespace: {self.namespace}")
        rospy.loginfo("ros_networktables_bridge is ready.")

    def create_ros_to_nt_subscriber(self, nt_key: str):
        """
        Create a ROS subscriber that listens to a ROS topic. The subscriber's callback function forwards messages
        to a NetworkTables key.
        :param nt_key: the networktables key that triggered the subscription
        :param topic_name: Name of the ROS topic. Must be in the root namespace (starts with /)
        """
        assert len(nt_key) > 0
        topic_name = self.absolute_topic_from_nt_key(nt_key)
        assert len(topic_name) > 0

        # Check if subscriber has already been created
        if nt_key in self.subscribers:
            self.unregister_subscriber(nt_key)

        # create GenericMessageSubscriber with a callback to ros_to_nt_callback
        subscriber = GenericMessageSubscriber(
            topic_name,
            lambda msg: self.ros_to_nt_callback(msg, nt_key),
            queue_size=self.queue_size,
        )

        # add to dictionary of subscribers
        self.subscribers[nt_key] = subscriber
        rospy.loginfo(f"Registering new subscriber: {topic_name}")

    def ros_to_nt_callback(self, msg, nt_key: str):
        """
        Callback function for forwarding ROS messages to NetworkTables.
        :param msg: ROS message.
        :param nt_key: the networktables key that triggered the subscription
        """
        try:
            # convert ROS message to JSON string
            raw_msg = ros_msg_to_string_json(msg)

            # send JSON string to corresponding NT entry
            self.ros_to_nt_subtable.getEntry(nt_key).setString(raw_msg)
        except BaseException as e:
            rospy.logerr_throttle(
                1.0, f"Exception in ros_to_nt_callback: {e}", exc_info=e
            )

    def create_nt_to_ros_publisher(self, nt_key: str, ros_msg_type) -> None:
        """
        Create a ROS publisher that listens to a NetworkTables key and forwards messages to a ROS topic.
        :param topic_name: Name of the ROS topic.
        :param ros_msg_type: ROS message type.
        """
        topic_name = self.absolute_topic_from_nt_key(nt_key)
        assert len(topic_name) > 0

        # Check if publisher has already been created
        if nt_key in self.publishers:
            self.unregister_publisher(nt_key)

        # create ROS publisher using message type received from NT
        pub = rospy.Publisher(topic_name, ros_msg_type, queue_size=self.queue_size, latch=True)

        # add to dictionary of publishers
        self.publishers[nt_key] = pub
        rospy.loginfo(f"Registering new publisher: {topic_name}")

    def absolute_topic_from_nt_key(self, key: str) -> str:
        # remove /nt_to_ros from the key
        table_divider = key.find("/", 1)
        topic_key = key[table_divider + 1 :]

        # convert to ROS topic
        topic_name = parse_nt_topic(topic_key)

        # convert to absolute topic
        topic_name = self.get_absolute_topic(topic_name)

        return topic_name

    def nt_to_ros_callback(
        self, entry: NetworkTableEntry, key: str, value: str, isNew: int
    ):
        """
        Callback function for forwarding NetworkTables messages to ROS.
        :param entry: NetworkTableEntry object.
        :param key: NetworkTables key.
        :param value: NetworkTables value.
        :param isNew: Flag indicating whether the value is new.
        """
        try:
            assert len(key) > 0

            # convert JSON string to ROS message and type
            ros_msg, ros_msg_type = string_json_to_ros_msg(value)
            if ros_msg is None or ros_msg_type is None:
                rospy.logwarn(f"Failed to parse message from {key}: {value}")
                return

            # create the publisher if that hasn't been done yet
            if key not in self.publishers:
                self.create_nt_to_ros_publisher(key, ros_msg_type)

            # get the corresponding publisher to the topic name
            pub = self.publishers[key]

            # publish the message
            pub.publish(ros_msg)
        except BaseException as e:
            # for some reason, exceptions only get printed within NT callbacks
            # if they are logged like this.
            rospy.logerr(
                f"Exception in nt_to_ros_callback: {e}. Received value: {value}",
                exc_info=e,
            )

    def unregister_publisher(self, nt_key: str) -> None:
        """
        Unregister a publisher by NetworkTables entry key.

        :param nt_key: NetworkTables entry key that points to a publisher
        """
        rospy.loginfo(f"Unregistering publish topic {nt_key}")
        if nt_key not in self.publishers:
            rospy.logwarn(f"Can't remove publisher {nt_key} as it's already removed.")
            return
        self.publishers[nt_key].unregister()
        del self.publishers[nt_key]

    def unregister_subscriber(self, nt_key: str) -> None:
        """
        Unregister a subscriber by NetworkTables entry key.

        :param nt_key: NetworkTables entry key that points to a subscriber
        """
        rospy.loginfo(f"Unregistering subscribe topic {nt_key}")
        if nt_key not in self.subscribers:
            rospy.logwarn(f"Can't remove subscriber {nt_key} as it's already removed.")
            return
        self.subscribers[nt_key].unregister()
        del self.subscribers[nt_key]

    def get_new_keys(self, local_keys: Set[str], remote_keys: Set[str]) -> Set[str]:
        """
        Comparing local NetworkTables keys and remote NetworkTables keys, get the keys that are new

        :return: Set of new keys
        """
        return remote_keys.difference(local_keys)

    def get_removed_keys(self, local_keys: Set[str], remote_keys: Set[str]) -> Set[str]:
        """
        Comparing local NetworkTables keys and remote NetworkTables keys, get the keys that were removed

        :return: Set of revmoed keys
        """
        return local_keys.difference(remote_keys)

    def get_topic_requests(self, topic_requests_entry: NetworkTableEntry) -> Set[str]:
        """
        Get a set of topics from the supplied topic requests entry.

        :return: Set of requested topic keys
        """
        return set(topic_requests_entry.getStringArray([]))

    def get_absolute_topic(self, topic_name: str) -> str:
        """
        Get topic in the root namespace.

        :param topic_name: absolute or relative topic
        :return: absolute topic
        """
        if topic_name[0] == "/":
            return topic_name
        else:
            return self.namespace + topic_name

    def check_new_pub_keys(self, new_pub_keys: Set[str]) -> None:
        for new_pub_key in new_pub_keys:
            # if there's already an entry listener for this new topic, skip it
            if new_pub_key in self.pub_listen_keys:
                continue

            # Add to set of initialized keys
            self.pub_listen_keys.add(new_pub_key)

            # create an entry listener for the new topic.
            new_pub_entry = self.nt_to_ros_subtable.getEntry(new_pub_key)
            handle = new_pub_entry.addListener(
                self.nt_to_ros_callback, NotifyFlags.UPDATE
            )
            self.pub_listen_handles[new_pub_key] = handle

            # forcefully call the callback since this is the first update
            self.nt_to_ros_callback(
                new_pub_entry, new_pub_key, new_pub_entry.getString(""), True
            )

    def check_removed_pub_keys(self, removed_pub_keys: Set[str]) -> None:
        for removed_pub_key in removed_pub_keys:
            # if the topic was already removed, skip it
            if removed_pub_key not in self.pub_listen_keys:
                continue

            # Remove from initialized keys
            self.pub_listen_keys.remove(removed_pub_key)

            removed_pub_entry = self.nt_to_ros_subtable.getEntry(removed_pub_key)
            removed_pub_entry.removeListener(self.pub_listen_handles[removed_pub_key])
            del self.pub_listen_handles[removed_pub_key]

            self.unregister_publisher(removed_pub_key)

    def check_new_sub_keys(self, new_sub_keys: Set[str]) -> None:
        for new_sub_key in new_sub_keys:
            # if there's already an subscriber for this new topic, skip it
            if new_sub_key in self.sub_listen_keys:
                continue

            # Add to set of initialized keys
            self.sub_listen_keys.add(new_sub_key)

            # create a subscriber for the new topic
            self.create_ros_to_nt_subscriber(new_sub_key)

    def check_removed_sub_keys(self, removed_sub_keys: Set[str]) -> None:
        for removed_sub_key in removed_sub_keys:
            # if the topic was already removed, skip it
            if removed_sub_key not in self.sub_listen_keys:
                continue

            # Remove from initialized keys
            self.sub_listen_keys.remove(removed_sub_key)

            self.unregister_subscriber(removed_sub_key)

    def run(self):
        """
        Main loop of the ROSNetworkTablesBridge. Continuously checks for new NetworkTables keys to create
        publishers and subscribers.
        """
        # create rate limiter object
        rate = rospy.Rate(1.0 / self.update_interval)

        # run until ROS signals to exit
        while not rospy.is_shutdown():
            # sleep by the specified rate
            rate.sleep()

            # get new publisher topics requested by the NT client
            pub_topic_requests = self.get_topic_requests(
                self.nt_to_ros_topic_requests_entry
            )
            new_pub_keys = self.get_new_keys(self.pub_listen_keys, pub_topic_requests)
            self.check_new_pub_keys(new_pub_keys)

            # get removed publisher topics requested by the NT client
            removed_pub_keys = self.get_removed_keys(
                self.pub_listen_keys, pub_topic_requests
            )
            self.check_removed_pub_keys(removed_pub_keys)

            # get new subscriber topics requested by the NT client
            sub_topic_requests = self.get_topic_requests(
                self.ros_to_nt_topic_requests_entry
            )
            new_sub_keys = self.get_new_keys(self.sub_listen_keys, sub_topic_requests)
            self.check_new_sub_keys(new_sub_keys)

            # get removed subscriber topics requested by the NT client
            removed_sub_keys = self.get_removed_keys(
                self.sub_listen_keys, sub_topic_requests
            )
            self.check_removed_sub_keys(removed_sub_keys)


if __name__ == "__main__":
    bridge = ROSNetworkTablesBridge()
    bridge.run()
