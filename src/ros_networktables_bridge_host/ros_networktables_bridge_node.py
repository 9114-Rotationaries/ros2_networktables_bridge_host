#!/usr/bin/env python
import sys
from typing import Any, Dict, Optional, Set
import rospy
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
        self.is_server = bool(rospy.get_param("~is_server", False))
        self.address = str(rospy.get_param("~address", "")).strip()
        self.port = int(rospy.get_param("~port", 1735))
        self.update_interval = float(rospy.get_param("~update_interval", 0.02))
        self.queue_size = int(rospy.get_param("~queue_size", 10))
        self.nt_warmup_time = float(rospy.get_param("~nt_warmup_time", 2.0))
        self.publish_warmup_time = rospy.Duration(
            rospy.get_param("~publish_warmup_time", 2.0)
        )
        self.publisher_non_empty_timeout = rospy.Duration(
            rospy.get_param("~publisher_non_empty_timeout", 0.25)
        )
        self.no_data_timeout = rospy.Duration(rospy.get_param("~no_data_timeout", 2.0))
        self.ros_to_nt_key = str(rospy.get_param("~ros_to_nt_table_key", "ros_to_nt"))
        self.nt_to_ros_key = str(rospy.get_param("~nt_to_ros_table_key", "nt_to_ros"))
        self.topics_request_key = str(rospy.get_param("~topics_request_key", "@topics"))
        self.time_sync_key = str(rospy.get_param("~time_sync_key", "@time"))

        self.open()

        # Initialize dictionaries and sets for publishers, subscribers, and keys
        self.publishers: Dict[str, rospy.Publisher] = {}
        self.pub_listen_keys: Set[str] = set()
        self.pub_listen_handles: Dict[str, int] = {}
        self.pub_initial_values: Optional[Dict[str, Any]] = {}
        self.pub_lock = Lock()

        self.subscribers: Dict[str, GenericMessageSubscriber] = {}
        self.sub_listen_keys: Set[str] = set()

        # Get the ROS namespace
        self.namespace: str = rospy.get_namespace()

        # start time of node
        self.start_time = rospy.Time(0)

        rospy.loginfo(f"Bridge namespace: {self.namespace}")
        rospy.loginfo("ros_networktables_bridge is ready.")

    def open(self):
        # Start NetworkTables server or client based on the is_server parameter
        if self.is_server:
            NetworkTables.startServer(listenAddress=self.address, port=self.port)
            rospy.loginfo(f"Starting server on port {self.port}")
            if len(self.address) > 0:
                rospy.loginfo(f"Address is {self.address}")
            assert NetworkTables.isServer()
        else:
            NetworkTables.startClient((self.address, self.port))
            rospy.loginfo(f"Connecting to {self.address}:{self.port}")
        NetworkTables.setUpdateRate(self.update_interval)

        # Get NetworkTables subtables
        self.ros_to_nt_subtable: NetworkTable = NetworkTables.getTable(
            self.ros_to_nt_key
        )
        self.nt_to_ros_subtable: NetworkTable = NetworkTables.getTable(
            self.nt_to_ros_key
        )
        self.ros_to_nt_topic_requests_table: NetworkTable = (
            self.ros_to_nt_subtable.getSubTable(self.topics_request_key)
        )
        self.nt_to_ros_topic_requests_table: NetworkTable = (
            self.nt_to_ros_subtable.getSubTable(self.topics_request_key)
        )
        self.time_sync_entry: NetworkTableEntry = self.ros_to_nt_subtable.getEntry(
            self.time_sync_key
        )

    def close(self):
        # Shutdown NetworkTables connection
        if self.is_server:
            NetworkTables.stopServer()
        else:
            NetworkTables.stopClient()

    def reopen(self):
        # Close and open NetworkTables connection
        self.close()
        rospy.sleep(0.25)
        self.open()
        rospy.sleep(self.nt_warmup_time)

    def update_time_sync(self):
        # Update time sync entry with current local time
        self.time_sync_entry.setDouble(rospy.Time.now().to_sec())

    def has_entries(self):
        # Are there any entries or subtables in the ROS <-> NT tables
        num_keys = (
            len(self.ros_to_nt_subtable.getKeys())
            + len(self.nt_to_ros_subtable.getKeys())
            + len(self.ros_to_nt_subtable.getSubTables())
            + len(self.nt_to_ros_subtable.getSubTables())
        )
        return num_keys > 0

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
        rospy.loginfo(f"Registering new subscriber: {topic_name} -> {nt_key}")

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
        pub = rospy.Publisher(
            topic_name, ros_msg_type, queue_size=self.queue_size, latch=True
        )

        # add to dictionary of publishers
        self.publishers[nt_key] = pub
        rospy.loginfo(f"Registering new publisher: {nt_key} -> {topic_name}")

    def get_key_base_name(self, key: str) -> str:
        # remove prefix from the key
        table_divider = key.rfind("/")
        return key[table_divider + 1 :]

    def absolute_topic_from_nt_key(self, key: str) -> str:
        topic_key = self.get_key_base_name(key)

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
            with self.pub_lock:
                self.publish_ros_message_from_nt_string(key, value)
        except rospy.exceptions.ROSException as e:
            rospy.logerr(e)
        except BaseException as e:
            # for some reason, exceptions only get printed within NT callbacks
            # if they are logged like this.
            rospy.logerr(
                f"Exception in nt_to_ros_callback: {e}. Received value: {value}",
                exc_info=e,
            )

    def publish_ros_message_from_nt_string(self, key: str, value: str) -> None:
        key = self.get_key_base_name(key)
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

        if pub.impl is not None and pub.impl.closed:
            rospy.logdebug(f"Publisher for {key} has closed. Skipping message publish.")
            return

        if self.pub_initial_values is None:
            # If initial values is None, the warmup time has exceeded.
            # Publish all new values
            pub.publish(ros_msg)
        else:
            if key in self.pub_initial_values:
                # If the we're still in warmup time, if an initial value has been registered
                # already, that means the value was updated during warmup. Publish this
                # new value
                pub.publish(ros_msg)
            else:
                # Otherwise, it's possible this value is stale, so don't publish it.
                self.pub_initial_values[key] = ros_msg
                rospy.logdebug(f"Initial value {key}: {ros_msg}")

        # remove initial values if start up time is exceeded.
        # This is done to save memory in case the initial value is large
        if (
            self.pub_initial_values is not None
            and rospy.Time.now() - self.start_time > self.publish_warmup_time
        ):
            rospy.logdebug("Publish warmup complete")
            self.pub_initial_values = None

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
        if topic_name[0] == "/":
            return topic_name
        else:
            return self.namespace + topic_name

    def wait_for_non_empty(self, entry: NetworkTableEntry) -> str:
        start_time = rospy.Time.now()
        value = ""
        while len(value) == 0:
            if (
                rospy.is_shutdown()
                or rospy.Time.now() - start_time > self.publisher_non_empty_timeout
            ):
                return ""
            value = entry.getString("")
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
        Main loop of the ROSNetworkTablesBridge. Continuously checks for new NetworkTables keys to create
        publishers and subscribers.
        """
        # create rate limiter object
        rate = rospy.Rate(1.0 / self.update_interval)

        # wait for NT entries to populate
        rospy.sleep(self.nt_warmup_time)
        self.start_time = rospy.Time.now()

        no_entries_timer = rospy.Time.now()

        # run until ROS signals to exit
        while not rospy.is_shutdown():
            # sleep by the specified rate
            rate.sleep()

            # push local time to NT
            self.update_time_sync()

            if self.has_entries():
                no_entries_timer = rospy.Time.now()
            else:
                if rospy.Time.now() - no_entries_timer > self.no_data_timeout:
                    rospy.logerr(
                        f"No data received for {self.no_data_timeout.to_sec()} seconds. Exiting."
                    )
                    sys.exit(1)

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
