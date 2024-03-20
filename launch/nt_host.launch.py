from launch import LaunchDescription
from launch_ros.actions import Node
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration

def generate_launch_description():
    return LaunchDescription([
        DeclareLaunchArgument(
            'nt_port',
            default_value='5800',
            description='NetworkTables port for server mode'
        ),
        Node(
            package='ros_networktables_bridge_host',
            executable='ros_networktables_bridge_node.py',
            name='ros_networktables_bridge',
            output='screen',
            respawn=True,
            parameters=[{
                'is_server': True,
                'port': LaunchConfiguration('nt_port'),
                'address': ''
            }]
        )
    ])
