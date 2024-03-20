INSTALLATION INSTRUCTIONS

1. compile ROS2
2. clone realsense, Isaac ROS Common, VSLAM, bridge, dependencies

(specific to 9114)

1. realsense node (initialized ASAP)
   1. Result: stereo infrared streams
2. VSLAM node, taking in the starting position of the robot, and realsense data
   1. Input: starting position of the robot (the position topic before autonomous mode starts), realsense stereo infrared streams
   2. Result: predicted position of the robot (maybe depth map too?)
3. ROS-json bridge
   1. Input: (sent to robot controller), calculated VSLAM position, requested velocities for omnidirectional robot
   2. Result: (received from robot controller), fused Pose, Velocity, goal Pose (no goal Velocity, do not consider final velocity in trajectory generation or following)
4. Static topics
   1. Result: transforms, occupancy grid
5. Nav2
   1. Input: fused Pose, Velocity, transforms, occupancy grid
   2. Result: requested velocities for omnidirectional robot