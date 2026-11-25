"""Owned Python-binding discovery helper; launched only by the admitted pool.

Stdout is JSONL. Public binding calls and a separate full-XTypes control stay
in this process; every result is auxiliary, never a SUT candidate or oracle.
"""
import dataclasses
import json
import os
from pathlib import Path
import sys
import time
import uuid


def main():
    from .discovery_tools import load_tools_environment
    config, role = json.loads(sys.argv[1]), sys.argv[2]
    generation = uuid.uuid4().hex
    ticks = int(Path('/proc/self/stat').read_text().rsplit(')', 1)[1].split()[19])

    def emit(event, **fields):
        print(json.dumps(dict(event=event, role=role, pid=os.getpid(), pgid=os.getpgrp(),
            process_start_ticks=ticks, observer_generation=generation,
            run_id=config['run_id'], domain_id=config['domain_id'],
            time_ns=time.time_ns(), monotonic_ns=time.monotonic_ns(), **fields), sort_keys=True), flush=True)

    entities = []
    try:
        manifest = load_tools_environment(config['projection_discovery_tools'])
        sys.path.insert(0, manifest['python_root'])
        from cyclonedds import builtin, domain, topic
        from cyclonedds.idl import IdlStruct
        from cyclonedds.qos import Qos, Policy
        import cyclonedds

        @dataclasses.dataclass
        class DiscoveryControl(IdlStruct, typename='r2f::DiscoveryControl'):
            value: str

        participant = domain.DomainParticipant(config['domain_id'])
        entities.append(participant)
        readers = {}
        for label in ('Participant', 'Topic', 'Publication', 'Subscription'):
            reader = builtin.BuiltinDataReader(participant, getattr(builtin, 'BuiltinTopicDcps' + label))
            entities.append(reader)
            readers['DCPS' + label] = reader
        # A reserved diagnostic topic stays identical on canonical replay.
        control_name = 'r2f_discovery_control'
        control = topic.Topic(participant, control_name, DiscoveryControl,
                              qos=Qos(Policy.History.KeepLast(7)))
        entities.append(control)
        maps = Path('/proc/self/maps').read_text()
        paths = {line.split(maxsplit=5)[5] for line in maps.splitlines()
                 if len(line.split(maxsplit=5)) == 6 and 'libddsc.so' in line.split(maxsplit=5)[5]}
        expected = Path(manifest['library']['path']).resolve()
        if not paths or any(Path(path).resolve() != expected for path in paths):
            raise ValueError('discovery_tools_mapped_library_mismatch')
        if not Path(cyclonedds.__file__).resolve().is_relative_to(Path(manifest['python_root'])):
            raise ValueError('discovery_tools_import_origin_mismatch')
        schema = {label: [field.name for field in dataclasses.fields(getattr(builtin, label))]
                  for label in ('DcpsParticipant', 'DcpsTopic', 'DcpsEndpoint')}
        emit('discovery_tools_ready', binding='cyclonedds-python',
             manifest=config['projection_discovery_tools'], version=manifest['version'],
             participant_guid=participant.guid.hex, schemas=schema,
             interpreter={'executable': sys.executable, 'version': sys.version},
             library=manifest['library'], mapped_library_paths=sorted(paths),
             control={'topic': control.get_name(), 'type': control.get_type_name(),
                      'type_support': 'Python_IdlStruct_generated_XTypes',
                      'participant_guid': participant.guid.hex, 'purpose': 'observation_positive_control',
                      'candidate_eligible': False})

        def sample_data(sample):
            info = sample.sample_info
            result = {'sample_class': type(sample).__name__, 'key': sample.key.hex,
                      'valid_data': bool(info.valid_data), 'instance_state': int(info.instance_state),
                      'source_timestamp': info.source_timestamp, 'taken_unix_ns': time.time_ns(),
                      'fields': [field.name for field in dataclasses.fields(sample)]}
            for key in ('participant_key', 'topic_name', 'type_name', 'participant_instance_handle'):
                if hasattr(sample, key):
                    value = getattr(sample, key)
                    result[key] = value.hex if isinstance(value, uuid.UUID) else value
            if info.valid_data:
                result['qos_repr'] = repr(sample.qos)
                result['qos'] = sample.qos.asdict()
                history = sample.qos[Policy.History]
                result['history'] = ({'status': 'observed', 'kind': 'KEEP_ALL', 'depth': None}
                    if history == Policy.History.KeepAll else
                    {'status': 'observed', 'kind': 'KEEP_LAST', 'depth': history.depth}
                    if isinstance(history, Policy.History.KeepLast) else {'status': 'unavailable_api'})
                result['type_id_status'] = ('available' if getattr(sample, 'type_id', None) is not None else 'unavailable_api')
            return result

        for line in sys.stdin:
            command = json.loads(line)
            if command['command'] == 'stop':
                break
            if command['command'] == 'observe':
                samples, errors = {}, []
                started = time.time_ns()
                for label, reader in readers.items():
                    samples[label] = []
                    try:
                        for _ in range(64):
                            values = reader.take(N=128)
                            samples[label].extend(sample_data(sample) for sample in values)
                            if len(values) < 128:
                                break
                        else:
                            raise RuntimeError('binding_take_drain_limit:' + label)
                    except Exception as exc:
                        errors.append({'layer': 'binding', 'topic': label,
                                       'error_type': type(exc).__name__, 'error': str(exc)})
                emit('discovery_python_observation', command_id=command['command_id'],
                     started_unix_ns=started, closed_unix_ns=time.time_ns(),
                     samples=samples, errors=errors, candidate_eligible=False,
                     binding_api='cyclonedds.builtin.BuiltinDataReader.take',
                     status='observed' if not errors else 'tool_error')
                emit('command_done', command_id=command['command_id'])
            elif command['command'] == 'cli':
                # Invoke the installed, unmodified CLI entry point. Catch real
                # background exceptions too: Click's exit_code alone misses them.
                from click.testing import CliRunner
                from cyclonedds.tools.cli.main import cli
                import threading
                import traceback
                errors = []
                previous = threading.excepthook
                def thread_error(args):
                    errors.append({'error_type': args.exc_type.__name__, 'error': str(args.exc_value),
                        'traceback': ''.join(traceback.format_exception(args.exc_type, args.exc_value, args.exc_traceback))})
                    previous(args)
                threading.excepthook = thread_error
                try:
                    for subcommand in ('ls', 'ps'):
                        errors.clear()
                        argv = [subcommand, '--id', str(config['domain_id']), '--runtime', '1s',
                                '--color', 'none', '--suppress-progress-bar', '--topic', '.*']
                        started = time.time_ns()
                        outcome = CliRunner().invoke(cli, argv, env={
                            'COLUMNS': '500', 'TERM': 'dumb', 'NO_COLOR': '1'}, catch_exceptions=True)
                        emit('discovery_cli_observation', command_id=command['command_id'],
                             subcommand=subcommand, argv=argv,
                             entrypoint='cyclonedds.tools.cli.main:cli',
                             invocation='Click_CliRunner_actual_unmodified_entrypoint_in_owned_helper',
                             started_unix_ns=started, closed_unix_ns=time.time_ns(),
                             exit_code=outcome.exit_code, stdout=outcome.stdout, stderr=outcome.stderr,
                             error=repr(outcome.exception) if outcome.exception else None,
                             thread_errors=list(errors), candidate_eligible=False)
                finally:
                    threading.excepthook = previous
                emit('command_done', command_id=command['command_id'])
            else:
                raise ValueError('unsupported_discovery_tools_command')
        for entity in reversed(entities):
            entity.__del__()
        entities.clear()
        load_tools_environment(config['projection_discovery_tools'])
        emit('worker_shutdown', binding_entities_close_requested=True)
    except Exception as exc:
        import traceback
        emit('discovery_tools_error', error_type=type(exc).__name__, reason=str(exc),
             traceback=traceback.format_exc(), candidate_eligible=False)
        raise
    finally:
        for entity in reversed(entities):
            entity.__del__()


if __name__ == '__main__':
    main()
