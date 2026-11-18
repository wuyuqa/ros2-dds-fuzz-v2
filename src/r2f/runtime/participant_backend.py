"""Participant lifecycle / lease facts and the read-only capability probe.

This module is deliberately inert: nothing here creates a ROS context, DDS
participant, endpoint or campaign.  It provides

* ``capability_probe`` - a read-only inventory of the host ROS/RMW/vendor
  stack (package versions, library SHA-256, public rclpy API availability,
  CycloneDDS C-API availability).  It runs in a child process, imports rclpy
  without creating a context, and only dlopen()s vendor libraries.
* ``validate_participant_config`` / ``normalize_lease`` - strict parameter
  domains for the participant lease surface.
* ``ParticipantLedger`` - an append-only fact ledger distinguishing
  participant lease activity, staleness, observed expiry, rediscovery,
  identity replacement and writer liveliness.  Every derived state names its
  basis; absence of activity never becomes an expiry claim.

Evidence rules:
* One long mismatch / missing sample cannot imply internal lease expiry.
* Writer liveliness is a separate channel and never participant-lease
  evidence (``participant_lease_evidence=False``).
* Guard receipts reject invalid input or unmet preconditions and are never
  SUT bugs, mutations or candidates.

Embedding contract (no second runner): the unified runner owns execution and
raw writing.  It passes its own ``emit`` callback and its own clock in, so
every receipt lands in the existing raw stream:

    ledger = ParticipantLedger(emit=raw.append, clock=time.monotonic_ns)
    ledger.register(identity, lease_config)      # declared, not created
    ledger.activity(identity, at_ns=..., source='builtin_participant_sample')
    ledger.state()                               # derived, basis-named

Nothing here creates a context/participant/endpoint, decides a stage, or
admits a candidate; a caller that wants a guard rejection to become a SUT
verdict must do so explicitly outside this module.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

SCHEMA = 'r2f.participant-lease/v1'
MODULE = 'r2f.runtime.participant_backend'
REPO = Path(__file__).resolve().parents[3]
ROS_PREFIX = Path('/opt/ros/jazzy')
INFINITE = 0x7fffffffffffffff
LEASE_SOURCES = ('vendor_config', 'graph_observation', 'builtin_observation',
                 'default_assumption')
ACTIVITY_SOURCES = ('builtin_participant_sample', 'discovery_announcement',
                   'endpoint_generation', 'harness_clock')
EXPIRY_BASES = ('builtin_instance_disposed', 'builtin_instance_no_writers')
LIVELINESS_KEYS = ('automatic', 'manual_by_participant', 'manual_by_topic')
GUARD_LAYERS = ('config_type', 'config_value', 'identity', 'precondition',
               'evidence', 'channel_confusion', 'unknown_capability')


class LeaseGuardError(ValueError):
    """Rejected input or unmet precondition; never a SUT defect."""

    def __init__(self, layer, reason, detail=None):
        if layer not in GUARD_LAYERS:
            raise ValueError('unknown lease layer: ' + str(layer))
        super().__init__(reason)
        self.layer = layer
        self.reason = reason
        self.detail = detail


def _now():
    return time.time_ns(), time.monotonic_ns()


def receipt(kind, **fields):
    wall, mono = _now()
    value = {'schema': SCHEMA, 'module': MODULE, 'receipt_kind': kind,
             'time_ns': wall, 'monotonic_ns': mono,
             'counts_toward_maturity': False, 'candidate_eligible': False,
             'mutation_receipt': False}
    value.update(fields)
    return value


def guard(reason, layer='precondition', **fields):
    value = receipt('participant_guard', reason=reason, layer=layer,
                    ok=False, accepted=False, sut_bug=False,
                    participant_created=False)
    value.update(fields)
    return value


def _sha256(path):
    try:
        return hashlib.sha256(Path(path).read_bytes()).hexdigest()
    except OSError:
        return None


def _package_version(name):
    xml = ROS_PREFIX / 'share' / name / 'package.xml'
    if not xml.is_file():
        return None
    text = xml.read_text(errors='replace')
    start = text.find('<version>')
    end = text.find('</version>', start)
    if start < 0 or end < 0:
        return None
    return text[start + len('<version>'):end].strip()


def normalize_lease(value):
    """Accept positive integer ns or the literal ``infinite``.

    Returns ``{'lease_duration_ns': int, 'infinite': bool}``.
    """
    if isinstance(value, str):
        if value.strip().lower() == 'infinite':
            return {'lease_duration_ns': INFINITE, 'infinite': True}
        raise LeaseGuardError('config_type', 'lease_duration_string_not_infinite', value)
    if isinstance(value, bool) or not isinstance(value, int):
        raise LeaseGuardError('config_type', 'lease_duration_must_be_integer_ns_or_infinite')
    if not 0 < value <= INFINITE:
        raise LeaseGuardError('config_value', 'lease_duration_out_of_range', value)
    return {'lease_duration_ns': value, 'infinite': value == INFINITE}


def _identity(value):
    if not isinstance(value, dict):
        raise LeaseGuardError('identity', 'identity_must_be_object')
    required = ('guid', 'vendor')
    missing = [key for key in required if not value.get(key)]
    if missing:
        raise LeaseGuardError('identity', 'identity_missing:' + ','.join(missing))
    guid = str(value['guid']).lower().replace('-', '')
    if len(guid) != 32 or any(c not in '0123456789abcdef' for c in guid):
        raise LeaseGuardError('identity', 'guid_must_be_16_bytes_hex')
    vendor = str(value['vendor'])
    if not vendor or len(vendor) > 64:
        raise LeaseGuardError('identity', 'vendor_invalid')
    generation = value.get('generation', 0)
    if isinstance(generation, bool) or not isinstance(generation, int) or generation < 0:
        raise LeaseGuardError('identity', 'generation_must_be_nonnegative_int')
    return {'guid': guid, 'vendor': vendor, 'generation': generation,
            'domain_id': value.get('domain_id'),
            'participant_name': value.get('participant_name')}


def validate_participant_config(config):
    """Validate the participant lease parameter surface.

    Unknown keys are rejected so an adapter cannot silently drop a caller's
    intent.  ``lease_source='default_assumption'`` is recorded as unknown, not
    as configured evidence.
    """
    if not isinstance(config, dict):
        return guard('config_must_be_object', layer='config_type')
    allowed = {'lease_duration', 'lease_source', 'source_reference',
               'identity', 'domain_id', 'participant_name', 'liveliness',
               'backend', 'vendor'}
    unknown = sorted(set(config) - allowed)
    if unknown:
        return guard('unknown_config_field:' + ','.join(unknown), layer='config_type')
    if 'lease_duration' not in config:
        return guard('lease_duration_required', layer='config_value')
    source = config.get('lease_source', 'vendor_config')
    if source not in LEASE_SOURCES:
        return guard('unknown_lease_source:' + str(source), layer='evidence')
    try:
        lease = normalize_lease(config['lease_duration'])
    except LeaseGuardError as exc:
        return guard(exc.reason, layer=exc.layer, detail=exc.detail)
    domain = config.get('domain_id')
    if domain is not None and (isinstance(domain, bool) or not isinstance(domain, int)
                              or not 0 <= domain <= 232):
        return guard('domain_id_out_of_range', layer='config_value')
    liveliness = config.get('liveliness')
    if liveliness is not None and liveliness not in LIVELINESS_KEYS:
        return guard('unsupported_liveliness:' + str(liveliness), layer='config_value')
    identity = None
    if config.get('identity') is not None:
        try:
            identity = _identity(config['identity'])
        except LeaseGuardError as exc:
            return guard(exc.reason, layer=exc.layer, detail=exc.detail)
    value = receipt('participant_config', accepted=True, ok=True,
                    identity=identity, source=source,
                    source_reference=config.get('source_reference'),
                    domain_id=domain, participant_name=config.get('participant_name'),
                    liveliness=liveliness, backend=config.get('backend'),
                    vendor=config.get('vendor'), **lease)
    value['configured'] = source != 'default_assumption' and lease['infinite'] is False
    if source == 'default_assumption':
        value['capability_status'] = 'unknown_not_configured_evidence'
        value['unknown_reason'] = 'default_assumption_is_not_configuration_evidence'
    return value


class ParticipantLedger:
    """Append-only participant facts for a single participant slot.

    ``events`` are the raw inputs; ``state`` is derived only from recorded
    events and always carries ``state_basis``.  The ledger never merges two
    GUIDs: a new GUID is an identity replacement, not a refresh.
    """

    def __init__(self, emit=None, clock=None):
        self.emit = emit
        self.clock = clock or time.monotonic_ns
        self.events = []
        self.current = None
        self.expired = {}
        self.replaced = {}

    def _record(self, value):
        self.events.append(value)
        if self.emit is not None:
            self.emit(value)
        return value

    def _require_current(self, identity, *, stage):
        try:
            ident = _identity(identity)
        except LeaseGuardError as exc:
            return None, guard(exc.reason, layer=exc.layer, stage=stage,
                               detail=exc.detail)
        if self.current is None:
            return None, guard('participant_not_registered', layer='precondition', stage=stage)
        if ident['guid'] != self.current['guid']:
            return None, guard('identity_not_current', layer='identity', stage=stage,
                               expected=self.current['guid'], observed=ident['guid'])
        return ident, None

    def register(self, identity, config=None, *, evidence=None):
        try:
            ident = _identity(identity)
        except LeaseGuardError as exc:
            return self._record(guard(exc.reason, layer=exc.layer, stage='register',
                                      detail=exc.detail))
        if self.current is not None and ident['guid'] != self.current['guid']:
            return self._record(guard('identity_already_registered', layer='precondition',
                                      stage='register', expected=self.current['guid'],
                                      observed=ident['guid']))
        config_value = None
        if config is not None:
            config_value = validate_participant_config(config)
            if not config_value.get('accepted'):
                config_value['stage'] = 'register'
                return self._record(config_value)
        value = receipt('participant_registered', stage='configured',
                        identity=ident,
                        lease=({k: config_value[k] for k in ('lease_duration_ns', 'infinite', 'source')}
                               if config_value else None),
                        config_evidence=config_value,
                        evidence=evidence or [],
                        participant_created=False,
                        registration_basis='declared_identity')
        if config_value is not None:
            value['configured'] = config_value.get('configured', False)
        self.current = ident
        return self._record(value)

    def activity(self, identity, *, at_ns=None, source='builtin_participant_sample',
                 evidence=None):
        ident, problem = self._require_current(identity, stage='activity')
        if problem:
            return self._record(problem)
        if source not in ACTIVITY_SOURCES:
            return self._record(guard('unknown_activity_source:' + str(source),
                                      layer='evidence', stage='activity'))
        at_ns = self.clock() if at_ns is None else at_ns
        if isinstance(at_ns, bool) or not isinstance(at_ns, int) or at_ns < 0:
            return self._record(guard('activity_time_invalid', layer='evidence', stage='activity'))
        value = receipt('participant_activity', stage='effect_observed',
                        identity=ident, activity_ns=at_ns, source=source,
                        evidence=evidence or [],
                        refresh_observed=True, refresh_claimed=True,
                        refresh_basis=source)
        self.expired.pop(ident['guid'], None)
        return self._record(value)

    def silence(self, identity, *, window_ns, observed_sources=(), evidence=None):
        """Record an observation gap.  Never an expiry inference."""
        ident, problem = self._require_current(identity, stage='silence')
        if problem:
            return self._record(problem)
        if isinstance(window_ns, bool) or not isinstance(window_ns, int) or window_ns <= 0:
            return self._record(guard('silence_window_invalid', layer='config_value',
                                      stage='silence'))
        value = receipt('participant_staleness', stage='effect_observed',
                        identity=ident, window_ns=window_ns,
                        observed_sources=list(observed_sources),
                        evidence=evidence or [],
                        status='stale_unconfirmed',
                        expiry_observed=False, expiry_inferred=False,
                        writer_liveliness_used=False,
                        inference_rule='absence_of_activity_does_not_prove_expiry')
        return self._record(value)

    def expiry(self, identity, *, at_ns=None, basis='builtin_instance_disposed',
               evidence=None):
        """Record an *observed* expiry sample (e.g. NOT_ALIVE_DISPOSED).

        Requires explicit builtin evidence with the matching GUID; a timeout
        or a long silence is rejected as insufficient.
        """
        ident, problem = self._require_current(identity, stage='expiry')
        if problem:
            return self._record(problem)
        if basis not in EXPIRY_BASES:
            return self._record(guard('unsupported_expiry_basis:' + str(basis),
                                      layer='evidence', stage='expiry'))
        evidence = evidence or []
        seen = False
        for item in evidence:
            if not isinstance(item, dict):
                continue
            if str(item.get('guid', '')).lower().replace('-', '') != ident['guid']:
                continue
            if item.get('instance_state') in ('NOT_ALIVE_DISPOSED', 'NOT_ALIVE_NO_WRITERS'):
                if item.get('valid_data') is False:
                    seen = True
        if not seen:
            return self._record(guard('expiry_requires_builtin_disposal_evidence',
                                      layer='evidence', stage='expiry',
                                      required_instance_state=list(EXPIRY_BASES),
                                      evidence_supplied=len(evidence)))
        at_ns = self.clock() if at_ns is None else at_ns
        value = receipt('participant_expiry', stage='effect_observed',
                        identity=ident, expiry_ns=at_ns, basis=basis,
                        evidence=evidence, expiry_observed=True,
                        expiry_basis='builtin_sample', expiry_inferred=False)
        self.expired[ident['guid']] = value
        return self._record(value)

    def rediscovery(self, identity, *, at_ns=None, source='builtin_participant_sample',
                    evidence=None):
        """Record re-appearance of the same GUID after a recorded expiry/stale."""
        ident, problem = self._require_current(identity, stage='rediscovery')
        if problem:
            return self._record(problem)
        if source not in ACTIVITY_SOURCES:
            return self._record(guard('unknown_activity_source:' + str(source),
                                      layer='evidence', stage='rediscovery'))
        had_gap = ident['guid'] in self.expired or any(
            item.get('receipt_kind') == 'participant_staleness'
            and item.get('identity', {}).get('guid') == ident['guid']
            for item in self.events)
        at_ns = self.clock() if at_ns is None else at_ns
        value = receipt('participant_rediscovery', stage='effect_observed',
                        identity=ident, rediscovery_ns=at_ns, source=source,
                        evidence=evidence or [],
                        prior_gap_observed=had_gap,
                        rediscovery_observed=True,
                        rediscovery_basis=source,
                        identity_generation_unchanged=True)
        if not had_gap:
            value['claim'] = 'reappearance_without_recorded_gap'
            value['prior_gap_evidence'] = False
        self.expired.pop(ident['guid'], None)
        return self._record(value)

    def replace_identity(self, new_identity, *, reason='identity_change', evidence=None):
        """Swap the current GUID; counts as replacement, never refresh."""
        try:
            new = _identity(new_identity)
        except LeaseGuardError as exc:
            return self._record(guard(exc.reason, layer=exc.layer, stage='replacement',
                                      detail=exc.detail))
        if self.current is not None and new['guid'] == self.current['guid']:
            return self._record(guard('replacement_requires_different_guid',
                                      layer='identity', stage='replacement'))
        old = self.current
        value = receipt('participant_identity_replacement', stage='effect_observed',
                        old_identity=old, new_identity=new, reason=reason,
                        evidence=evidence or [],
                        replacement_observed=True,
                        counts_as_refresh=False,
                        old_lease_reused=False,
                        new_generation=(new['generation'] if new else None))
        if old is not None:
            self.replaced[old['guid']] = value
        self.current = new
        return self._record(value)

    def writer_liveliness(self, identity, *, policy, at_ns=None, source='publisher_assert_liveliness',
                          evidence=None, missed=False):
        """Writer liveliness channel; never participant lease evidence."""
        ident, problem = self._require_current(identity, stage='writer_liveliness')
        if problem:
            return self._record(problem)
        if policy not in LIVELINESS_KEYS:
            return self._record(guard('unsupported_liveliness:' + str(policy),
                                      layer='config_value', stage='writer_liveliness'))
        at_ns = self.clock() if at_ns is None else at_ns
        value = receipt('writer_liveliness', channel='writer_liveliness',
                        participant_lease_evidence=False,
                        identity=ident, policy=policy, liveliness_ns=at_ns,
                        source=source, missed=bool(missed),
                        evidence=evidence or [],
                        refresh_claimed=False,
                        inference_rule='writer_liveliness_is_not_participant_lease')
        return self._record(value)

    def infer_expiry_from_mismatch(self, *, mismatch_ns=None, evidence=None):
        """Refuse the invalid inference; returns a guard, not an expiry."""
        return self._record(guard('mismatch_or_silence_cannot_prove_lease_expiry',
                                  layer='channel_confusion', stage='inference',
                                  mismatch_ns=mismatch_ns,
                                  evidence=evidence or [],
                                  expiry_inferred=False,
                                  required_basis=list(EXPIRY_BASES)))

    def state(self):
        if self.current is None:
            return receipt('participant_state', status='unregistered',
                           state_basis='no_registration')
        guid = self.current['guid']
        last = None
        for item in reversed(self.events):
            if item.get('receipt_kind') == 'participant_activity' \
                    and item.get('identity', {}).get('guid') == guid:
                last = item
                break
        expiry = self.expired.get(guid)
        rediscovered = None
        if expiry is not None:
            for item in reversed(self.events):
                if item.get('receipt_kind') == 'participant_rediscovery' \
                        and item.get('identity', {}).get('guid') == guid:
                    rediscovered = item
                    break
        if expiry is not None and rediscovered is None:
            status, basis = 'expired_observed', 'builtin_disposal_sample'
        elif expiry is not None:
            status, basis = 'rediscovered', 'expiry_then_reappearance'
        elif last is not None:
            status, basis = 'active_observed', 'last_activity_sample'
        else:
            status, basis = 'registered_no_activity', 'registration_only'
        return receipt('participant_state', status=status, state_basis=basis,
                       identity=self.current, last_activity=last,
                       expiry=expiry, rediscovery=rediscovered,
                       expiry_inferred=False)


def _probe_code():
    return r'''
import ctypes, hashlib, inspect, json, os, pathlib
prefix = pathlib.Path('/opt/ros/jazzy')
import platform, sys
out = {'ros_distro': os.environ.get('ROS_DISTRO'),
       'platform': platform.platform(), 'python_version': platform.python_version(),
       'python_executable': sys.executable,
       'public_api': {}, 'ddsc': {}, 'errors': []}
try:
    import rclpy
    from rclpy.node import Node
    from rclpy.subscription import Subscription
    from rclpy.publisher import Publisher
    from rclpy.subscription_content_filter_options import ContentFilterOptions
    sig = inspect.signature(Node.create_subscription)
    out['public_api'] = {
        'rclpy_import': True,
        'rclpy_module': getattr(rclpy, '__file__', None),
        'create_subscription_has_content_filter_options': 'content_filter_options' in sig.parameters,
        'subscription_set_content_filter': hasattr(Subscription, 'set_content_filter'),
        'subscription_get_content_filter': hasattr(Subscription, 'get_content_filter'),
        'subscription_is_cft_enabled': hasattr(Subscription, 'is_cft_enabled'),
        'publisher_assert_liveliness': hasattr(Publisher, 'assert_liveliness'),
        'content_filter_options_type': ContentFilterOptions.__name__,
        'content_filter_options_fields': list(getattr(ContentFilterOptions, '_fields', [])),
        'context_created': False, 'node_created': False, 'participant_created': False}
except Exception as exc:
    out['public_api'] = {'rclpy_import': False,
                         'error': type(exc).__name__ + ':' + str(exc),
                         'context_created': False, 'node_created': False,
                         'participant_created': False}
    out['errors'].append('rclpy:' + type(exc).__name__)
base = prefix / 'lib' / 'x86_64-linux-gnu'
preload_names = ('libiceoryx_platform.so', 'libiceoryx_hoofs.so',
                 'libiceoryx_posh.so', 'libiceoryx_binding_c.so')
loaded = []
try:
    for name in preload_names:
        path = base / name
        if not path.is_file():
            out['errors'].append('preload_missing:' + name)
            continue
        ctypes.CDLL(str(path), mode=ctypes.RTLD_GLOBAL)
        loaded.append({'name': name, 'path': str(path), 'realpath': os.path.realpath(path),
                       'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
    library = base / 'libddsc.so.0'
    lib = ctypes.CDLL(str(library), mode=ctypes.RTLD_GLOBAL)
    symbols = ('dds_create_participant', 'dds_delete', 'dds_get_guid',
               'dds_create_reader', 'dds_create_topic', 'dds_take',
               'dds_return_loan', 'dds_builtintopic_free_participant',
               'dds_qget_liveliness', 'dds_qget_history')
    out['ddsc'] = {'library': str(library), 'realpath': os.path.realpath(library),
                   'sha256': hashlib.sha256(library.read_bytes()).hexdigest(),
                   'preloads': loaded,
                   'symbols': {name: hasattr(lib, name) for name in symbols},
                   'available': all(hasattr(lib, name) for name in symbols),
                   'builtin_participant_topic': 0x7fff0000 + 1,
                   'participant_created': False, 'context_created': False}
except Exception as exc:
    out['ddsc'] = {'available': False, 'error': type(exc).__name__ + ':' + str(exc),
                   'preloads': loaded, 'participant_created': False}
    out['errors'].append('ddsc:' + type(exc).__name__)
for package in ('rclpy', 'rmw_cyclonedds_cpp', 'rmw_fastrtps_cpp', 'cyclonedds', 'fastrtps'):
    xml = prefix / 'share' / package / 'package.xml'
    if xml.is_file():
        text = xml.read_text(errors='replace')
        start = text.find('<version>')
        end = text.find('</version>', start)
        out.setdefault('packages', {})[package] = {
            'version': text[start+9:end].strip() if start >= 0 and end > start else None,
            'package_xml_sha256': hashlib.sha256(xml.read_bytes()).hexdigest()}
print(json.dumps(out, sort_keys=True))
'''


def capability_probe(timeout_sec=30):
    """Run the read-only inventory in a child interpreter.

    No ROS context/node/participant is created.  A missing rclpy/import or a
    missing vendor library is recorded as an explicit capability gap, never
    guessed at.
    """
    env = {k: v for k, v in os.environ.items()
           if k in ('HOME', 'LANG', 'LC_ALL', 'TMPDIR', 'PATH', 'LD_LIBRARY_PATH')}
    env['PYTHONDONTWRITEBYTECODE'] = '1'
    libraries = [str(ROS_PREFIX / 'lib' / 'x86_64-linux-gnu'),
                 str(ROS_PREFIX / 'lib')]
    env['LD_LIBRARY_PATH'] = os.pathsep.join(
        libraries + ([env['LD_LIBRARY_PATH']] if env.get('LD_LIBRARY_PATH') else []))
    # ROS Python packages live under the distro prefix; the probe must see the
    # same rclpy the live runner would, without executing setup.bash.
    python_site = str(ROS_PREFIX / 'lib' / 'python3.12' / 'site-packages')
    env['PYTHONPATH'] = os.pathsep.join(
        [python_site] + ([env['PYTHONPATH']] if env.get('PYTHONPATH') else []))
    command = [sys.executable, '-B', '-c', _probe_code()]
    started = time.monotonic_ns()
    try:
        proc = subprocess.run(command, env=env, text=True, capture_output=True,
                              timeout=timeout_sec)
    except (OSError, subprocess.SubprocessError) as exc:
        value = guard('capability_probe_failed:' + type(exc).__name__,
                      layer='unknown_capability', probe='participant_filter')
        # A probe that cannot run is an unknown capability, never "absent".
        value.update(capability_status='probe_failed', ok=False,
                     participant_created=False, context_created=False,
                     public_filter_api=None, native_ddsc_available=None,
                     native_route={'backend': 'ctypes-cyclonedds',
                                   'available': None,
                                   'reason': 'probe_failed',
                                   'live_create_destroy': 'unknown'})
        return value
    finished = time.monotonic_ns()
    value = receipt('capability_probe', probe='participant_filter_v1',
                    command=' '.join(command[:2]) + ' -c <probe>',
                    exit_code=proc.returncode,
                    elapsed_ns=finished - started,
                    participant_created=False, context_created=False,
                    source_identity=_module_identity())
    if proc.returncode != 0:
        value.update(ok=False, capability_status='probe_failed',
                     stderr_tail=proc.stderr.strip().splitlines()[-8:])
        return value
    try:
        data = json.loads(proc.stdout)
    except ValueError:
        value.update(ok=False, capability_status='probe_output_invalid',
                     stdout_head=proc.stdout[:400])
        return value
    value.update(ok=True, capabilities=data,
                 capability_status='observed',
                 public_filter_api=data.get('public_api', {}).get(
                     'create_subscription_has_content_filter_options') is True,
                 native_ddsc_available=data.get('ddsc', {}).get('available') is True)
    api = data.get('public_api', {})
    value['public_api_gaps'] = [name for name in
                                ('subscription_set_content_filter',
                                 'subscription_get_content_filter',
                                 'subscription_is_cft_enabled',
                                 'publisher_assert_liveliness')
                                if api.get(name) is not True]
    ddsc = data.get('ddsc', {})
    if ddsc.get('available'):
        value['native_route'] = {
            'backend': 'ctypes-cyclonedds',
            'library': ddsc.get('realpath'),
            'library_sha256': ddsc.get('sha256'),
            'abi_basis': 'declared from CyberDDS ddsc headers; revalidate before live use',
            'compiler_required': False,
            'live_create_destroy': 'not_run_no_live_go'}
    else:
        value['native_route'] = {'backend': 'ctypes-cyclonedds',
                                 'available': False,
                                 'reason': ddsc.get('error') or 'symbols_missing',
                                 'live_create_destroy': 'capability_gap'}
    return value


def _module_identity():
    path = Path(__file__)
    return {'module': MODULE, 'path': str(path), 'sha256': _sha256(path),
            'python': sys.version.split()[0]}
