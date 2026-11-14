"""Public ROS2 endpoint runtime. Imports remain usable without ROS installed."""

def run_pair(config, output_dir):
    from .ros2_pair import run_pair as impl
    return impl(config, output_dir)
