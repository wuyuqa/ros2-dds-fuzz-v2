"""Offline only: every subprocess/namespace probe is replaced, no tc/nft runs."""
import json
from types import SimpleNamespace

import pytest
from r2f.runtime import discovery_network as network
from r2f.runtime import topology_environment_observation as obs


@pytest.fixture
def harness(monkeypatch):
    calls = []
    resolutions = []
    payloads = [json.dumps([{'kind': 'noqueue', 'handle': '0:', 'root': True}]),
                '[]', '{"nftables": []}']
    state = dict(missing=False, returncode=0, failure=None, wrong_ns=False)
    monkeypatch.setenv('PATH', '/usr/bin:/bin')

    def which(tool, path=None):
        resolutions.append((tool, path))
        if tool.startswith('/'):
            return tool if not state['missing'] else None
        if path is not None and not state['missing']:
            return '/usr/sbin/' + tool
        return None

    def isolation(**kwargs):
        if state['wrong_ns']:
            raise network.NetworkIsolationError('fixture_wrong_namespace')
        return {'observed_net_namespace': 'net:[91]'}

    def run(argv, **kwargs):
        calls.append((argv, kwargs))
        if state['failure']:
            raise state['failure']
        return SimpleNamespace(returncode=state['returncode'],
                               stdout=payloads[len(calls)-1], stderr='fixture')

    monkeypatch.setattr(obs.shutil, 'which', which)
    monkeypatch.setattr(network, 'assert_isolated_namespace', isolation)
    monkeypatch.setattr(network.subprocess, 'run', run)
    return state, calls, resolutions, payloads


def test_missing_sbin_resolves_real_command_and_preserves_actual_json(harness):
    state, calls, resolutions, payloads = harness
    rows = obs._KernelProbe().read_network()
    assert [r['argv'] for r in rows] == list(obs._COMMANDS)
    assert [r['stdout'] for r in rows] == payloads
    assert all(r['tool_resolution'] == 'fixed_system_path' for r in rows)
    assert [r['executed_argv'] for r in rows] == [x[0] for x in calls]
    assert [x[0][0] for x in calls] == ['/usr/sbin/tc', '/usr/sbin/tc', '/usr/sbin/nft']
    assert all(x[1]['timeout'] == 5 and x[1]['check'] is False for x in calls)
    assert obs.audit_neutral_readback(rows)['audit_neutral']


def test_inherited_path_takes_precedence(harness, monkeypatch):
    monkeypatch.setattr(obs.shutil, 'which', lambda tool, path=None: tool if tool.startswith('/') else '/usr/bin/'+tool)
    rows = obs._KernelProbe().read_network()
    assert all(r['tool_resolution'] == 'inherited_path' for r in rows)
    assert rows[0]['executed_argv'][0] == '/usr/bin/tc'


def test_missing_tools_stay_unknown_without_subprocess(harness):
    state, calls, _, _ = harness
    state['missing'] = True
    with pytest.raises(obs.ObservationUnavailable) as exc:
        obs._KernelProbe().read_network()
    assert exc.value.receipt['failed_command']['error'] == 'tool_missing'
    assert exc.value.receipt['commands'] == []
    assert not calls


@pytest.mark.parametrize('failure', [None, PermissionError('fixture-denied'),
                                   network.subprocess.TimeoutExpired('fixture', 5)])
def test_command_failure_stays_unknown_with_resolved_receipt(harness, failure):
    state, calls, _, _ = harness
    state.update(returncode=2, failure=failure)
    with pytest.raises(obs.ObservationUnavailable) as exc:
        obs._KernelProbe().read_network()
    failed = exc.value.receipt['failed_command']
    assert failed['argv'] == obs._COMMANDS[0]
    assert failed['executed_argv'][0] == '/usr/sbin/tc'
    assert len(calls) == 1


def test_wrong_namespace_blocks_before_subprocess(harness):
    state, calls, _, _ = harness
    state['wrong_ns'] = True
    with pytest.raises(obs.ObservationUnavailable, match='fixture_wrong_namespace'):
        obs._KernelProbe().read_network()
    assert not calls


@pytest.mark.parametrize('payload', ['invalid', '{"nftables":[],"nftables":[]}',
                                    '{"nftables":NaN}', '[]'])
def test_invalid_json_cannot_be_neutral(harness, payload):
    harness[3][2] = payload
    rows = obs._KernelProbe().read_network()
    with pytest.raises(obs.ObservationUnavailable):
        obs.audit_neutral_readback(rows)


def test_non_neutral_readbacks_are_not_replaced_by_configuration(harness):
    harness[3][0] = '[{"kind":"netem","handle":"0:","root":true}]'
    with pytest.raises(obs.ObservationUnavailable, match='qdisc_not_exclusively_noqueue'):
        obs.audit_neutral_readback(obs._KernelProbe().read_network())


def test_resolution_does_not_modify_process_path(harness):
    import os
    before = dict(os.environ)
    obs._KernelProbe().read_network()
    assert dict(os.environ) == before


def test_namespace_is_revalidated_for_every_command(harness, monkeypatch):
    count = []
    def isolation(**kwargs):
        count.append(kwargs)
        if len(count) == 2:
            raise network.NetworkIsolationError('fixture_namespace_changed')
        return {'observed_net_namespace': 'net:[91]'}
    monkeypatch.setattr(network, 'assert_isolated_namespace', isolation)
    with pytest.raises(obs.ObservationUnavailable, match='fixture_namespace_changed') as exc:
        obs._KernelProbe().read_network()
    assert len(count) == 2 and len(harness[1]) == 1
    assert len(exc.value.receipt['commands']) == 1
