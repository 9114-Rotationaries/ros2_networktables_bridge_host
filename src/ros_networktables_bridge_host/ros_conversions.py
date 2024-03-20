import rclpy
import json
from importlib import import_module
from rosbridge_library.internal import message_conversion
from roslib.message import get_message_class


def ros_msg_to_msg_dict(msg):
    msg_dict = message_conversion.extract_values(msg)
    msg_dict["_type"] = msg._type # fully qualified message type name
    return msg_dict


def ros_msg_to_string_json(msg):
    msg_dict = ros_msg_to_msg_dict(msg)
    msg_json = json.dumps(msg_dict)
    return msg_json


def string_json_to_msg_dict(msg_json: str):
    return json.loads(msg_json)


def remove_type_fields(msg_dict: dict):
    new_dict = {}
    for key in msg_dict.keys():
        if key == "_type":
            continue
        elif type(msg_dict[key]) == dict:
            new_dict[key] = remove_type_fields(msg_dict[key])
        elif type(msg_dict[key]) == list:
            new_dict[key] = []
            if len(msg_dict[key]) > 0:
                # ROS arrays are all the same type. Only check the first value if it has a _type field
                first_value = msg_dict[key][0]
                if type(first_value) == dict:
                    for value in msg_dict[key]:
                        assert type(value) == dict, value
                        new_dict[key].append(remove_type_fields(value))
                else:
                    new_dict[key] = msg_dict[key]
        else:
            new_dict[key] = msg_dict[key]
    return new_dict


def msg_dict_to_ros_type(msg_dict, logger):
    if "_type" not in msg_dict:
        logger.error("JSON message must include a '_type' field.")
        return None, None

    msg_type = msg_dict["_type"]
    msg_dict = remove_type_fields(msg_dict)
    try:
        ros_msg_type = get_message(msg_type)
    except Exception as e:
        logger.error(f"Invalid message type: {msg_type}. Error: {e}")
        return None, None

    return ros_msg_type, msg_dict


def msg_dict_to_ros_msg(msg_dict, msg_cls):
    assert msg_cls is not None
    msg = msg_cls()
    message_conversion.populate_instance(msg_dict, msg)
    return msg


def string_json_to_ros_msg(string_json: str, logger):
    if len(string_json) == 0:
        return None, None
    msg_dict = string_json_to_msg_dict(string_json)
    ros_msg_type, msg_dict = msg_dict_to_ros_type(msg_dict, logger)
    if ros_msg_type is None or msg_dict is None:
        return None, None
    ros_msg = msg_dict_to_ros_msg(msg_dict, ros_msg_type)
    return ros_msg, ros_msg_type


def parse_nt_topic(nt_ros_topic: str) -> str:
    return nt_ros_topic.replace("\\", "/")


def convert_to_nt_topic(ros_topic: str) -> str:
    return ros_topic.replace("/", "\\")


def get_msg_class(cache, msg_type_name: str, logger):
    if msg_type_name not in cache:
        try:
            msg_class = get_message(msg_type_name)
            cache[msg_type_name] = msg_class
        except Exception as e:
            logger.error(f"Error getting message class for {msg_type_name}: {e}")
            return None
    return cache[msg_type_name]
