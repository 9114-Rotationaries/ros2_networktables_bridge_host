#!/usr/bin/env python
import sys
from typing import Any, Dict, Optional, Set
import rclpy
from rclpy.node import Node
from rclpy.duration import Duration
from networktables import (
    NetworkTables,
    NetworkTable,
    NetworkTablesInstance,
    NetworkTableEntry,
)
from threading import Lock
from ros_conversions import (
    ros_msg_to_string_json,
    string_json_to_ros_msg,
    parse_nt_topic,
) # todo: pass a logger to functions that are going to log things
from generic_message_subscriber import GenericMessageSubscriber


NotifyFlags = NetworkTablesInstance.NotifyFlags


class ROSNetworkTablesBridge(Node):
    def __init__(self):
        super().__init__('ros_networktables_bridge')
        # changed parameters :)
        self.declare_parameters(
            namespace='',
            parameters=[
                ('is_server', False),
                ('address', ''),
                ('port', 1735),
                ('update_interval', 0.02),
                ('queue_size', 10),
                ('nt_warmup_time', 2.0),
                ('publish_warmup_time', 2.0),
                ('publisher_non_empty_timeout', 0.25),
                ('no_data_timeout', 2.0),
                ('ros_to_nt_table_key', 'ros_to_nt'),
                ('nt_to_ros_table_key', 'nt_to_ros'),
                ('topics_request_key', '@topics'),
                ('time_sync_key', '@time'),
            ]
        )
        self.is_server = self.get_parameter('is_server').get_parameter_value().bool_value
        self.address = self.get_parameter('address').get_parameter_value().string_value
        self.port = self.get_parameter('port').get_parameter_value().integer_value
        self.update_interval = self.get_parameter('update_interval').get_parameter_value().double_value
        self.queue_size = self.get_parameter('queue_size').get_parameter_value().integer_value
        self.nt_warmup_time = self.get_parameter('nt_warmup_time').get_parameter_value().double_value
        self.publish_warmup_time = Duration(seconds=self.get_parameter('publish_warmup_time').get_parameter_value().double_value)
        self.publisher_non_empty_timeout = Duration(seconds=self.get_parameter('publisher_non_empty_timeout').get_parameter_value().double_value)
        self.no_data_timeout = Duration(seconds=self.get_parameter('no_data_timeout').get_parameter_value().double_value)
        self.ros_to_nt_key = self.get_parameter('ros_to_nt_table_key').get_parameter_value().string_value
        self.nt_to_ros_key = self.get_parameter('nt_to_ros_table_key').get_parameter_value().string_value
        self.topics_request_key = self.get_parameter('topics_request_key').get_parameter_value().string_value
        self.time_sync_key = self.get_parameter('time_sync_key').get_parameter_value().string_value

        self.open()

        # initialize dictionaries and sets for publishers, subscribers, and keys
        self.publishers: Dict[str, rclpy.publisher.Publisher] = {}
        self.pub_listen_keys: Set[str] = set()
        self.pub_listen_handles: Dict[str, int] = {}
        self.pub_initial_values: Optional[Dict[str, Any]] = {}
        self.pub_lock = Lock()

        self.subscribers: Dict[str, GenericMessageSubscriber] = {}
        self.sub_listen_keys: Set[str] = set()

        # get the ROS namespace
        self.namespace = self.get_namespace()

        # start time of node
        self.start_time = self.get_clock().now()

        # I code in proportional font :)
        self.get_logger().info(f"Bridge namespace: {self.namespace}")
        self.get_logger().info("ros_networktables_bridge is ready.")

    def open(self):
        # Start NetworkTables server or client based on the is_server parameter
        if self.is_server:
            NetworkTables.startServer(listenAddress=self.address, port=self.port)
            self.get_logger().info(f"Starting server on port {self.port}")
            if self.address:
                self.get_logger().info(f"Address is {self.address}")
            assert NetworkTables.isServer()
        else:
            NetworkTables.startClient((self.address, self.port))
            self.get_logger().info(f"Connecting to {self.address}:{self.port}")
        NetworkTables.setUpdateRate(self.update_interval)

        # Get NetworkTables subtables
        self.ros_to_nt_subtable = NetworkTables.getTable(self.ros_to_nt_key)
        self.nt_to_ros_subtable = NetworkTables.getTable(self.nt_to_ros_key)
        self.ros_to_nt_topic_requests_table = self.ros_to_nt_subtable.getSubTable(self.topics_request_key)
        self.nt_to_ros_topic_requests_table = self.nt_to_ros_subtable.getSubTable(self.topics_request_key)
        self.time_sync_entry = self.ros_to_nt_subtable.getEntry(self.time_sync_key)

    def close(self):
        # Shutdown NetworkTables connection
        if self.is_server:
            NetworkTables.stopServer()
        else:
            NetworkTables.stopClient()

    def reopen(self):
        # Close and open NetworkTables connection
        self.close()
        rclpy.spin_once(self, timeout_sec=0.25)
        self.open()
        rclpy.spin_once(self, timeout_sec=self.nt_warmup_time)

    def update_time_sync(self):
        # Update time sync entry with current local time
        current_time = self.get_clock().now()
        # Convert ROS2 time to seconds as FP
        self.time_sync_entry.setDouble(current_time.to_msg().sec + current_time.to_msg().nanosec / 1e9)

    def has_entries(self): # no code changes to port to ROS2 :)
        # Check if there are any entries or subtables in the ROS <-> NT tables
        num_keys = (
            len(self.ros_to_nt_subtable.getKeys()) +
            len(self.nt_to_ros_subtable.getKeys()) +
            len(self.ros_to_nt_subtable.getSubTables()) +
            len(self.nt_to_ros_subtable.getSubTables())
        )
        return num_keys > 0

    def create_ros_to_nt_subscriber(self, nt_key: str):
        """
        ROS2: Create a ROS subscriber that listens to a ROS topic. The subscriber's callback function forwards messages
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

        # Create GenericMessageSubscriber with a callback to ros_to_nt_callback in ROS2
        # Note: The GenericMessageSubscriber needs to be adapted for ROS2
        subscriber = GenericMessageSubscriber(
            topic_name,
            lambda msg: self.ros_to_nt_callback(msg, nt_key),
            queue_size=self.queue_size
        )

        # Add to the dictionary of subscribers
        self.subscribers[nt_key] = subscriber
        self.get_logger().info(f'Registering new subscriber: {topic_name} -> {nt_key}')

    def ros_to_nt_callback(self, msg, nt_key: str):
        """
        Callback function for forwarding ROS messages to NetworkTables in ROS2.
        :param msg: ROS message.
        :param nt_key: the NetworkTables key that triggered the subscription.
        """
        try:
            # Convert ROS message to JSON string. Ensure this function is adapted for ROS2.
            raw_msg = ros_msg_to_string_json(msg)

            # Send JSON string to corresponding NT entry
            self.ros_to_nt_subtable.getEntry(nt_key).setString(raw_msg)
        except Exception as e:
            # ROS2 equivalent of rospy.logerr (no throttle)
            self.get_logger().error(f"Exception in ros_to_nt_callback: {e}")

    def create_nt_to_ros_publisher(self, nt_key: str, ros_msg_type) -> None:
        """
        Create a ROS2 publisher that listens to a NetworkTables key and forwards messages to a ROS topic.
        :param nt_key: NetworkTables key used to identify the ROS topic.
        :param ros_msg_type: ROS message type for the topic.
        """
        topic_name = self.absolute_topic_from_nt_key(nt_key)
        assert len(topic_name) > 0

        # Check if a publisher for this nt_key already exists
        if nt_key in self.publishers:
            self.unregister_publisher(nt_key)

        # Create ROS2 publisher using the message type received from NT
        pub = self.create_publisher(ros_msg_type, topic_name, self.queue_size)

        # Add the publisher to the dictionary of publishers
        self.publishers[nt_key] = pub
        self.get_logger().info(f"Registering new publisher: {nt_key} -> {topic_name}")

    def get_key_base_name(self, key: str) -> str:
        """Remove prefix from the key."""
        table_divider = key.rfind("/")
        return key[table_divider + 1:]

    def absolute_topic_from_nt_key(self, key: str) -> str:
        """Convert a NetworkTables key to an absolute ROS topic name."""
        topic_key = self.get_key_base_name(key)

        # Convert to ROS topic
        topic_name = parse_nt_topic(topic_key)

        # Convert to absolute topic
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
            with self.pub_lock:
                self.publish_ros_message_from_nt_string(key, value)
        except rclpy.exceptions.ROSException as e:
            self.get_logger().error(str(e))
        except Exception as e:
            # For exceptions within NetworkTables callbacks, log the error in python?
            self.get_logger().error(f"Exception in nt_to_ros_callback: {e}. Received value: {value}", exc_info=e)

    def publish_ros_message_from_nt_string(self, key: str, value: str) -> None:
        key = self.get_key_base_name(key)
        assert len(key) > 0

        # Convert JSON string to ROS message and type
        # Ensure that string_json_to_ros_msg is adapted for ROS2
        ros_msg, ros_msg_type = string_json_to_ros_msg(value)
        if ros_msg is None or ros_msg_type is None:
            self.get_logger().warn(f"Failed to parse message from {key}: {value}")
            return

        # Create the publisher if it hasn't been done yet
        if key not in self.publishers:
            self.create_nt_to_ros_publisher(key, ros_msg_type)

        # Get the corresponding publisher to the topic name
        pub = self.publishers[key]
        # removed the checking for closed publisher, as ROS2 handles it I think

        # Check for initial values and publish
        if self.pub_initial_values is None or key in self.pub_initial_values:
            pub.publish(ros_msg)
        else:
            # If we're still within the warmup period, and this key hasn't been initialized,
            # don't publish yet.
            self.pub_initial_values[key] = ros_msg
            self.get_logger().debug(f"Initial value {key}: {ros_msg}")

        # Remove initial values if the startup time is exceeded, to save memory
        if self.pub_initial_values is not None and self.get_clock().now() - self.start_time > Duration(seconds=self.publish_warmup_time):
            self.get_logger().debug("Publish warmup complete")
            self.pub_initial_values = None

    def unregister_publisher(self, nt_key: str) -> None:
        """
        Unregister a publisher by NetworkTables entry key.

        :param nt_key: NetworkTables entry key that points to a publisher
        """
        self.get_logger().info(f"Unregistering publish topic {nt_key}")
        if nt_key not in self.publishers:
            self.get_logger().warn(f"Can't remove publisher {nt_key} as it's already removed.")
            return
        # no need to unregister in ROS2
        del self.publishers[nt_key]

    def unregister_subscriber(self, nt_key: str) -> None:
        """
        Unregister a subscriber by NetworkTables entry key.

        :param nt_key: NetworkTables entry key that points to a subscriber
        """
        self.get_logger.info(f"Unregistering subscribe topic {nt_key}")
        if nt_key not in self.subscribers:
            self.get_logger.warn(f"Can't remove subscriber {nt_key} as it's already removed.")
            return
        # no need to unregister in ROS2
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

    def get_topic_requests(self, topic_requests_table: NetworkTable) -> Set[str]:
        """
        Get a set of topics from the supplied topic requests table.

        :return: Set of requested topic keys
        """
        requests = set()
        for entry_name in topic_requests_table.getKeys():
            entry = topic_requests_table.getEntry(entry_name)
            if entry.getBoolean(False):
                requests.add(entry_name)
        return requests

    def get_absolute_topic(self, topic_name: str) -> str:
        """
        Get topic in the root namespace.

        :param topic_name: absolute or relative topic
        :return: absolute topic
        """
        if topic_name.startswith("/"):
            return topic_name
        else:
            return self.get_namespace() + topic_name

    def wait_for_non_empty(self, entry: NetworkTableEntry) -> str: 
        # this thing's original implementation was not good
        # as it couldn't be interrupted. But this isn't much better. 
        #I'll watch out for it.
        start_time = self.get_clock().now()
        value = ""
        while len(value) == 0:
            if self.get_clock().now() - start_time > rclpy.duration.Duration(seconds=self.publisher_non_empty_timeout).to_sec():
                return ""
            value = entry.getString("")
            rclpy.spin_once(self, timeout_sec=self.update_interval)  # Add brief spin to allow interruption
        return value

    def check_new_pub_keys(self, new_pub_keys: Set[str]) -> None:
        for new_pub_key in new_pub_keys:
            # if there's already an entry listener for this new topic, skip it
            if new_pub_key in self.pub_listen_keys:
                continue

            # Add to set of initialized keys
            self.pub_listen_keys.add(new_pub_key)

            # forcefully call the callback since this is the first update
            new_pub_entry = self.nt_to_ros_subtable.getEntry(new_pub_key)
            self.nt_to_ros_callback(
                new_pub_entry, new_pub_key, self.wait_for_non_empty(new_pub_entry), True
            )

            # create an entry listener for the new topic.
            handle = new_pub_entry.addListener(
                self.nt_to_ros_callback, NotifyFlags.UPDATE
            )
            self.pub_listen_handles[new_pub_key] = handle

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

            with self.pub_lock:
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
        Main loop for ROS2. Continuously checks for new NetworkTables keys to create
        publishers and subscribers.
        """
        # Create rate limiter object for ROS2
        rate = self.create_rate(1.0 / self.update_interval)

        # Wait for NT entries to populate, adapting for ROS2
        rclpy.sleep(self.nt_warmup_time)
        self.start_time = self.get_clock().now()

        no_entries_timer = self.get_clock().now()

        # Run until ROS2 signals to exit
        while rclpy.ok():
            # sleep specified rate
            rate.sleep()

            # Push local time to NT
            self.update_time_sync()

            if self.has_entries():
                no_entries_timer = self.get_clock().now()
            else:
                if self.get_clock().now() - no_entries_timer > Duration(seconds=self.no_data_timeout):
                    self.get_logger().error(
                        f"No data received for {self.no_data_timeout} seconds. Exiting."
                    )
                    rclpy.shutdown()
                    return

            # get new publisher topics requested by the NT client
            pub_topic_requests = self.get_topic_requests(
                self.nt_to_ros_topic_requests_table
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
                self.ros_to_nt_topic_requests_table
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
