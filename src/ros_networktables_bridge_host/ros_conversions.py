import rospy
import json
from importlib import import_module
from rosbridge_library.internal import message_conversion
from roslib.message import get_message_class


def ros_msg_to_msg_dict(msg):
    msg_dict = message_conversion.extract_values(msg)
    msg_dict["_type"] = getattr(msg, "_type", None)  # type: ignore
    return msg_dict


def ros_msg_to_string_json(msg):
    msg_dict = ros_msg_to_msg_dict(msg)
    msg_json = json.dumps(msg_dict)
    return msg_json


def string_json_to_msg_dict(msg_json: str):
    return json.loads(msg_json)


def remove_type_fields(msg_dict):
    new_dict = {}
    for key in msg_dict.keys():
        if key == "_type":
            continue
        elif type(msg_dict[key]) == dict:
            new_dict[key] = remove_type_fields(msg_dict[key])
        else:
            new_dict[key] = msg_dict[key]
    return new_dict


def msg_dict_to_ros_type(msg_dict):
    if "_type" not in msg_dict:
        rospy.logerr("JSON message must include a '_type' field.")
        return None, None

    msg_type = msg_dict["_type"]
    msg_dict = remove_type_fields(msg_dict)
    ros_msg_type = get_message_class(msg_type)

    if ros_msg_type is None:
        rospy.logerr("Invalid message type: %s", msg_type)
        return None, None

    return ros_msg_type, msg_dict


def msg_dict_to_ros_msg(msg_dict, msg_cls):
    assert msg_cls is not None
    msg = msg_cls()
    message_conversion.populate_instance(msg_dict, msg)
    return msg


def string_json_to_ros_msg(string_json: str):
    if len(string_json) == 0:
        return None, None
    msg_dict = string_json_to_msg_dict(string_json)
    ros_msg_type, msg_dict = msg_dict_to_ros_type(msg_dict)
    if ros_msg_type is None or msg_dict is None:
        return None, None
    ros_msg = msg_dict_to_ros_msg(msg_dict, ros_msg_type)
    return ros_msg, ros_msg_type


def parse_nt_topic(nt_ros_topic: str) -> str:
    return nt_ros_topic.replace("\\", "/")


def convert_to_nt_topic(ros_topic: str) -> str:
    return ros_topic.replace("/", "\\")


def get_msg_class(cache, msg_type_name: str):
    if msg_type_name not in cache:
        connection_header = msg_type_name.split("/")
        ros_pkg = connection_header[0] + ".msg"
        msg_type = connection_header[1]
        msg_class = getattr(import_module(ros_pkg), msg_type)
        cache[msg_type_name] = msg_class
    return cache[msg_type_name]
