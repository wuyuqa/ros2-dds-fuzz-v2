"""Explicit native ABI probe; no ROS init, participant, endpoint or DDS credit."""
import json
import os
from pathlib import Path
import subprocess

import pytest


@pytest.mark.skipif(os.environ.get('R2F_NATIVE_DDS_TESTS') != '1',
                    reason='opt-in qualified Fast DDS local native probe')
def test_fresh_process_serializes_empty_local_roots_without_ros_init(tmp_path):
    selected = os.environ.get('R2F_FASTDDS_LOCAL_MANIFEST')
    if not selected:
        pytest.skip('R2F_FASTDDS_LOCAL_MANIFEST must select a qualified build')
    from r2f.instrumentation.fastdds_peer.activation import environment_for_reference
    from r2f.instrumentation.fastdds_peer.build import digest
    from r2f.runtime.environment import activated_environment
    manifest = Path(selected).resolve(strict=True)
    reference = {'observation_mode': 'instrumentation', 'manifest_path': str(manifest),
                 'manifest_sha256': digest(manifest)}
    _, environment = activated_environment({})
    environment = environment_for_reference(environment, reference)
    code = '''import json,sys
import rclpy
from r2f.runtime.fastdds_local_api import sample_roots
from r2f.runtime.rcl_take_bridge import RclTakeBridge
reference=json.loads(sys.argv[1])
# Load qualified RMW/generated support, without ROS init or DDS entities.
bridge=RclTakeBridge(message_type='std_msgs/msg/UInt8MultiArray',instrumentation=reference)
try:
    print(json.dumps(sample_roots(reference,215),sort_keys=True))
finally:
    bridge.close()
'''
    result = subprocess.run(['/usr/bin/python3', '-B', '-c', code, json.dumps(reference)],
                            env=environment, text=True, capture_output=True, timeout=30)
    (tmp_path/'native.stdout').write_text(result.stdout)
    (tmp_path/'native.stderr').write_text(result.stderr)
    assert result.returncode == 0, (result.returncode, result.stderr)
    observed = json.loads(result.stdout)
    assert observed['schema'] == 'r2f.fastdds-local-participant-roots/v1'
    assert observed['domain_id'] == 215
    assert observed['complete'] is True
    assert observed['roots'] == [] and observed['count'] == 0
    assert observed['candidate_eligible'] is False
