"""Optional vendor observations of the existing discovery workers; no launcher."""
from pathlib import Path

from r2f.instrumentation.cyclonedds import (
    RMW, build_trace_config, capture_process_provenance, collect_trace,
    environment_for_mode,
)


class ParticipantLeaseObservation:
    def __init__(self, config, output_dir, environment, role_environments, emit):
        self.config, self.emit = config, emit
        self.traces, self.processes = {}, {}
        mode = config.get('observation_mode', 'blackbox')
        if mode not in ('blackbox', 'instrumentation'):
            raise ValueError('participant_lease_observation_mode_invalid')
        self.environment = environment_for_mode(environment, mode='blackbox')
        self.role_environments = {role: dict(env) for role, env in role_environments.items()}
        if mode == 'instrumentation':
            if config['rmw'] != RMW or config.get('rmws'):
                raise ValueError('participant_lease_trace_requires_single_vendor_cyclone')
            directory = Path(output_dir).resolve() / 'participant-leases'
            directory.mkdir(exist_ok=False)
            for role in ('writer', 'reader'):
                base = Path(self.role_environments[role]['CYCLONEDDS_URI']).read_text()
                trace = build_trace_config(directory / (role + '.trace.log'),
                    domain_id=config['domain_id'], base_xml=base, observe_participant_lifecycle=True)
                self.traces[role] = trace
                self.role_environments[role].update(trace.environment)
        self.emit('participant_lease_execution_metadata', observation_mode=mode,
                  trace_configs={role: trace.to_dict() for role, trace in self.traces.items()},
                  effective_input_sha256=config.get('effective_input_sha256'),
                  target_id=config['target_id'], target_revision=config['target_revision'],
                  arm=config['arm'], repeat=config['repeat'], candidate_eligible=False)

    def capture(self, role, endpoint):
        if role in self.traces:
            proof = capture_process_provenance(endpoint['pid'], trace_config=self.traces[role])
            self.processes[role] = [proof]
            self.emit('participant_lease_process_observed', worker_role=role,
                      endpoint_created_ref=endpoint['raw_event_ref'], process=proof,
                      candidate_eligible=False)

    def close(self):
        for role, config in self.traces.items():
            try:
                trace = collect_trace(config, processes=self.processes.get(role, []),
                                      control_task={'kind': 'participant_lease_supporting_observer'})
                self.emit('participant_lease_trace_closed', worker_role=role,
                          **{key: trace[key] for key in ('raw_path', 'raw_sha256', 'config', 'processes',
                                                        'vendor_process_verified', 'build_provenance')},
                          candidate_eligible=False)
            except (OSError, ValueError, KeyError) as exc:
                self.emit('participant_lease_trace_unknown', worker_role=role,
                          reason=type(exc).__name__ + ':' + str(exc), candidate_eligible=False)
