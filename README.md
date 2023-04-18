# ros_networktables_bridge_host

ros_networktables_bridge_host is a ROS package that bridges ROS (Robot Operating System) to the roboRIO using NetworkTables and JSON formatted NT entries. This project aims to provide a seamless integration between FRC (FIRST Robotics Competition) robots using WPILib and ROS, enabling advanced robot control, sensor integration, and autonomy. It's a cousin project to the Java client <https://github.com/frc-88/ROSNetworkTablesBridge>

# Features

- Connects WPILib-based FRC robots with ROS using NetworkTables
- Uses JSON formatted NT entries for standardized communication
- Supports custom and standard ROS messages

# Prerequisites

- ROS environment setup (tested with ROS Noetic)
- rosbridge_library

# Installation

- Install rosbridge_suite dependency
    - If you're on Ubuntu 20, run `sudo apt-get install ros-noetic-rosbridge-suite`
    - Otherwise, clone it into your workspace:
```bash
git clone https://github.com/RobotWebTools/rosbridge_suite.git -b ros1
```

- Clone into your catkin workspace:

```bash
git clone https://github.com/frc-88/ros_networktables_bridge_host.git
```

- Rebuild the workspace: `catkin_make`

# Example usage

- Create a launch file:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<launch>
    <include file="$(find ros_networktables_bridge_host)/launch/nt_client.launch">
        <arg name="nt_host" value="10.0.88.2"/>
    </include>
</launch>
```

- Launch the node: `roslaunch path/to/your/file.launch`

# Contributing

Contributions are welcome! Please follow the guidelines in the CONTRIBUTING.md file in <https://github.com/frc-88/ROSNetworkTablesBridge>
