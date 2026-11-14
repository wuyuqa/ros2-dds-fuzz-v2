# Python source overlay for the new repository. This is not a colcon install overlay.
# Source /opt/ros/jazzy/setup.bash first, then source this file.
_r2f_overlay_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
export PYTHONPATH="${_r2f_overlay_root}/src${PYTHONPATH:+:${PYTHONPATH}}"
export R2F_OVERLAY_KIND=python_source_overlay
export R2F_OVERLAY_SOURCE="${_r2f_overlay_root}"
unset _r2f_overlay_root
