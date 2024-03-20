from setuptools import setup

package_name = 'ros_networktables_bridge_host'

setup(
    name=package_name,
    version='0.0.0',
    packages=[package_name],
    data_files=[
        ('share/' + package_name, ['package.xml']),
        ('share/ament_index/resource_index/packages',
            ['resource/' + package_name]),
    ],
    install_requires=['setuptools', 'rclpy', 'pynetworktables'],
    zip_safe=True,
    maintainer='woz4tetra',
    maintainer_email='woz4tetra@gmail.com',
    description='The ros_networktables_bridge_host package',
    license='MIT',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            # Define console script entry points for your node executables
        ],
    },
)
