cmake_minimum_required(VERSION 2.8.3)
project(ros_networktables_bridge_host)

find_package(catkin REQUIRED COMPONENTS
    rospy
)

catkin_python_setup()

catkin_package(
    CATKIN_DEPENDS rospy
)

catkin_install_python(PROGRAMS src/ros_networktables_bridge_host/ros_networktables_bridge_node.py
    DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
)
