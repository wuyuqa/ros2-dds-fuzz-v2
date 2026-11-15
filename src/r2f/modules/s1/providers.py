"""S1 services over the existing runtime and raw-evidence adapters.

Preparation describes inputs only. Execution dependencies are passed explicitly
and never become part of a scenario, capsule, catalog or effective input hash.
"""
from copy import deepcopy

from r2f.core import ExecutionServices, Scenario, canonical_hash, effective_input_hash
from .targets import EXECUTION_ORDER


_KINDS = dict(zip(EXECUTION_ORDER, ('late_joiner', 'late_joiner',
                                  'discovery_drop', 'endpoint_qos_variation')))


def _accepts(scenario, targets, carrier=None):
    if not isinstance(scenario, Scenario):
        return False
    target = targets.get(scenario.target_id)
    if target is None:
        return False
    try:
        # Revalidate mutable scenario fields against the registered declaration.
        Scenario.from_dict(scenario.to_dict())
        return (canonical_hash(scenario.target.to_dict()) == canonical_hash(target.to_dict())
                and scenario.mutation_kind == _KINDS.get(scenario.target_id)
                and (carrier is None or scenario.carrier == carrier))
    except (ValueError, KeyError, TypeError, AttributeError):
        return False


class S1MutationProvider:
    def __init__(self, targets):
        self._targets = deepcopy(targets)

    def prepare(self, scenario):
        """Return the existing deterministic layer description, without I/O."""
        if not _accepts(scenario, self._targets):
            raise ValueError('unsupported S1 mutation scenario')
        parameters = scenario.mutation_parameters
        layer = {'layer': scenario.carrier, 'declared_mutation': scenario.mutation_kind}
        if scenario.target_id == EXECUTION_ORDER[2]:
            layer.update(implementation='isolated_netns_nft_discovery_udp_drop', parameters=parameters)
        elif scenario.target_id == EXECUTION_ORDER[3]:
            layer['parameters'] = parameters
        else:
            if 'reader_delay_sec' not in parameters:
                raise ValueError('prepared late_joiner requires submitted reader_delay_sec')
            layer['submitted_reader_delay_sec'] = parameters['reader_delay_sec']
        return {'mutation_kind': scenario.mutation_kind, 'carrier': scenario.carrier,
                'mutation_parameters': parameters, 'input_layer_receipts': [layer]}


class S1Carrier:
    def __init__(self, targets, carrier):
        self._targets = deepcopy(targets)
        self._carrier = carrier

    def accepts(self, scenario):
        return _accepts(scenario, self._targets, self._carrier)

    def apply(self, scenario, prepared, *, execution):
        """Invoke the existing runtime once and return its complete envelope.

        prepared is the capsule's effective_input, including its exact final
        input and layer descriptions. In particular replay never prepares it
        again. Exceptions and even malformed runtime responses are left to the
        existing caller's persistence and failure-classification boundary.
        """
        if not self.accepts(scenario):
            raise ValueError('unsupported S1 carrier scenario')
        expected = scenario.effective_input(final_input=prepared['final_input'],
                                            input_layer_receipts=prepared['input_layer_receipts'],
                                            source_manifest=prepared['source_manifest'])
        if canonical_hash(prepared) != canonical_hash(expected):
            raise ValueError('carrier prepared input differs from scenario')
        digest = effective_input_hash(prepared)
        config = deepcopy(execution['config'])
        if canonical_hash(config.get('effective_input_sha256')) != canonical_hash(digest):
            raise ValueError('carrier effective input hash mismatch')
        # Canonical JSON distinguishes bool/int and int/float, including
        # nested values. Python equality would admit False as repeat=0.
        bindings = {'final_input': prepared['final_input'],
                    'input_layer_receipts': prepared['input_layer_receipts'],
                    'source_manifest': prepared['source_manifest'],
                    'scenario': scenario.to_dict()}
        if any(canonical_hash(config.get(key)) != canonical_hash(value)
               for key, value in bindings.items()):
            raise ValueError('carrier config/capsule input binding mismatch')
        for key in ('target_id', 'target_revision', 'rmw', 'arm', 'repeat', 'seed',
                    'mutation_kind', 'carrier'):
            if canonical_hash(config.get(key)) != canonical_hash(expected[key]):
                raise ValueError('carrier execution identity mismatch:' + key)
        for key, value in prepared['final_input'].items():
            if canonical_hash(config.get(key)) != canonical_hash(value):
                raise ValueError('carrier final input mismatch:' + key)
        runtime = execution['runtime']
        if not callable(runtime):
            raise ValueError('carrier requires explicit runtime callable')
        return runtime(config, execution['output_dir'])


class S1FactProducer:
    def produce(self, raw):
        # Import only at invocation: projection/recovery adapters already import
        # runtime_adapter helpers, and module registration must stay offline.
        from r2f.campaign.runtime_adapter import facts_from_events
        return facts_from_events(raw['events'], raw['runtime'], raw['config'])


class S1Oracle:
    def evaluate(self, scenario, facts):
        from .oracles import evaluate_primary
        return evaluate_primary(facts, scenario.target_id)


def service_bindings(targets):
    mutation = S1MutationProvider(targets)
    facts, oracle = S1FactProducer(), S1Oracle()
    carriers = {name: S1Carrier(targets, name) for name in ('ros2_endpoint', 'wire_proxy')}
    return {target_id: ExecutionServices(mutation, carriers['wire_proxy' if target_id == EXECUTION_ORDER[2]
                                                           else 'ros2_endpoint'], facts, oracle)
            for target_id in targets}
