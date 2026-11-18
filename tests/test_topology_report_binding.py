"""Report-only wiring contracts. All execution/qualification is explicitly mocked.

No ROS, DDS, namespace, canonical callable or live artifact is used by these
fixtures. Real route/plan/capsule/checkpoint/hash/path validation is retained.
"""
from copy import deepcopy
import json
from pathlib import Path
import shutil
from unittest.mock import patch

import pytest

from r2f.campaign import runner
from r2f.campaign.checkpoint import make_checkpoint_hook
from r2f.campaign.corpus import corpus_from_profile
from r2f.campaign.feedback import FeedbackLog
from r2f.campaign.scheduler import ExploratoryScheduler
from r2f.campaign.topology_search import descriptor
from r2f.core.evidence import canonical_hash, raw_sha256
from r2f.reports import run_report as audit
from r2f.reports.candidate_report import exploratory_candidate_ledger


def repo_root():
    for parent in Path(__file__).resolve().parents:
        if (parent / 'config/s1_fuzz/profiles/model-bfs-topology.json').is_file():
            return parent
    raise AssertionError('repository not found')


def read(path):
    return json.loads(Path(path).read_text())


def write(path, value):
    Path(path).write_text(json.dumps(value, sort_keys=True, indent=2) + '\n')


@pytest.fixture(scope='module')
def model_report(tmp_path_factory):
    root = tmp_path_factory.mktemp('topology-report-model')
    doc = read(repo_root() / 'config/s1_fuzz/profiles/model-bfs-topology.json')
    doc['rmws'] = doc['rmws'][:1]
    doc['operator_whitelist'] = ['endpoint.count', 'queue.depth', 'relation.pair']
    classes = {('endpoint.count', 'add'), ('queue.depth', 'one'), ('relation.pair', 'RR')}
    doc['topology_search']['samples'] = [s for s in doc['topology_search']['samples']
                                       if (s['operator_id'], s['class_id']) in classes]
    doc['seed_specs'] = [s for s in doc['seed_specs'] if s['seed_id'] == 'topology.multi_endpoint']
    doc['declared_seed_specs_union'] = deepcopy(doc['seed_specs'])
    doc['cycles'][0]['seed_specs'] = deepcopy(doc['seed_specs'])
    doc['budget'].update(max_inputs=4, max_seconds=300)
    source = dict(source_repo=str(repo_root()), head='a' * 40, behavior_tree_sha256='b' * 64,
                  source_frozen=True, dirty_behavior_paths=[])
    corpus = corpus_from_profile(doc, cycle_id='topology-authored')
    feedback = FeedbackLog()
    scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=41, warmup=0, policy='bfs',
                                    sampling_kinds=doc.get('sampling_kinds'),
                                    sampling_weight=doc.get('sampling_weight'))
    scheduler.configure_search(doc['search'], descriptor(doc['topology_search'], doc['operator_whitelist']))
    hook = make_checkpoint_hook(root, profile=doc, cycle_id='topology-authored', source_manifest=source)
    with patch.object(runner, 'source_manifest', return_value=source):
        runner.run_exploratory_cycle(root, corpus=corpus, feedback=feedback, scheduler=scheduler,
                                    continuation=None, profile=doc, max_inputs=4, execute=False,
                                    runtime=None, run_id='four-row-mock', checkpoint_hook=hook)
    return root


@pytest.fixture
def four_rows(model_report, tmp_path, monkeypatch):
    root = tmp_path / 'report'
    shutil.copytree(model_report, root)
    for file in root.rglob('*.json'):
        file.write_text(file.read_text().replace(str(model_report), str(root)))
    checkpoint = read(root / 'checkpoint.json')
    checkpoint['checkpoint_sha256'] = canonical_hash({k: v for k, v in checkpoint.items()
                                                      if k != 'checkpoint_sha256'})
    write(root / 'checkpoint.json', checkpoint)
    report = read(root / 'report.json')
    report.update(runtime_boundary='isolated_netns', runtime_formal=True)
    source = deepcopy(report['source_manifest'])
    # Only the execution/reingest/qualification boundary is mocked. The saved
    # inputs, plans, capsules and rejection model are the real offline producer.
    for row in report['rows']:
        if row.get('generation_status') == 'rejected':
            continue
        cfgpath = Path(row['runtime_config_path'])
        cfg = read(cfgpath)
        cfg.update(execution_boundary={'enabled': True}, live_execution_go=True)
        write(cfgpath, cfg)
        runtime = Path(row['runtime_result_path']).parent
        runtime.mkdir()
        raw = runtime / 'raw.jsonl'
        raw.write_text('{"event":"explicit_mock_execution_not_real_DDS"}\n')
        row.update(actual_run=True, execution_stage='judged', oracle_verdict='unknown',
                   raw_path=str(raw), raw_sha256=raw_sha256(raw), runtime_status='completed',
                   runtime_reason=None, observer_complete=True,
                   typed_facts={'mock_only': True}, oracle={'verdict': 'unknown'},
                   state_projection={'status': 'observed'})
        # A separate saved result is the mocked reingest authority, not the
        # mutable row under test. Tampering row/raw must disagree with it.
        write(row['runtime_result_path'], {'mock_ingest': deepcopy(row)})
    monkeypatch.setattr(audit, 'source_manifest', lambda path: deepcopy(source))
    monkeypatch.setattr(audit, 'ingest_execution', lambda scenario, config, result, capsule, **kw:
                        deepcopy(result['mock_ingest']))
    import r2f.campaign.replay as replay
    monkeypatch.setattr(replay, '_qualification', lambda *a, **kw:
                        dict(real_ros_execution=True, qualification_errors=[], mock_only=True))
    def boundary(root, row, report):
        if row.get('actual_run') is True:
            return dict(status='qualified', reason_code='mock_only', errors=[], eligible=True)
        return dict(status='unknown', reason_code=audit.BOUNDARY_AUDIT_PENDING, errors=[], eligible=False)
    monkeypatch.setattr(audit, 'boundary_qualification_for_row', boundary)
    ledger = exploratory_candidate_ledger([r for r in report['rows'] if r.get('actual_run')])
    ledger_path = root / 'candidate_ledger.json'
    write(ledger_path, ledger)
    report['candidate_ledger_path'] = str(ledger_path)
    report['candidate_ledger_sha256'] = canonical_hash(ledger)
    report['budget'].update(attempted=2, executed=2, actual=2, unique_actual=2)
    report['last_state']['executed'] = 2
    report['report_sha256'] = audit.exploratory_report_digest(report)
    write(root / 'report.json', report)
    return root, report


def run_audit(fixture):
    root, report = fixture
    report['report_sha256'] = audit.exploratory_report_digest(report)
    write(root / 'report.json', report)
    return audit.audit_exploratory_report(root / 'report.json')


def test_four_rows_two_model_rejects_two_mock_actual_unknown(four_rows):
    result = run_audit(four_rows)
    assert result['evidence_valid'], result['errors']
    assert result['rows_verified'] == result['rows_total'] == 4
    assert result['unknown_count'] == 2
    assert not result['all_oracles_pass'] and not result['sealed']
    assert result['seal'] == 'not_applicable_exploratory'
    assert read(four_rows[0] / result['candidate_ledger'])['natural_candidate_count'] == 0


@pytest.mark.parametrize('damage', [
    'submitted_hash', 'normalized_hash', 'status_only', 'silent', 'numeric_widening',
    'generic_equal_labels', 'remove_markers', 'final_config', 'raw', 'raw_rehash',
    'source', 'path_escape', 'path_other_case', 'plan', 'capsule', 'boundary_options',
    'intent_false', 'missing_result', 'missing_raw', 'unqualified', 'effect_unqualified',
])
def test_topology_binding_tampering_remains_invalid_after_outer_resign(four_rows, monkeypatch, damage):
    root, report = four_rows
    row = report['rows'][1]
    if damage in {'submitted_hash', 'normalized_hash'}:
        row['binding'][{'submitted_hash': 'submitted_config_sha256',
                        'normalized_hash': 'normalized_config_sha256'}[damage]] = 'f' * 64
    elif damage == 'status_only':
        row['binding'] = {'status': 'preserved', 'silent_normalization': False}
    elif damage == 'silent': row['binding']['silent_normalization'] = True
    elif damage == 'numeric_widening': row['binding']['numeric_widening_fields'] = ['topology_program']
    elif damage in {'generic_equal_labels', 'remove_markers'}:
        row['binding'] = dict(expected_config_sha256='a' * 64, observed_config_sha256='a' * 64,
                              silent_normalization=False)
        if damage == 'remove_markers':
            for key in ('runtime_capability', 'topology_generation', 'generation_binding'):
                row.pop(key, None)
            report['model'].pop('targets', None)
            report['model'].pop('rules', None)
            report['identity']['model_sha256'] = canonical_hash(report['model'])
    elif damage in {'final_config', 'source', 'boundary_options', 'intent_false'}:
        cfg = read(row['runtime_config_path'])
        if damage == 'final_config': cfg['topology_program']['actions'] = []
        elif damage == 'source': cfg['source_manifest']['head'] = 'c' * 40
        elif damage == 'boundary_options': cfg['execution_boundary']['timeout_sec'] = 999
        else: cfg['live_execution_go'] = False
        write(row['runtime_config_path'], cfg)
    elif damage in {'raw', 'raw_rehash'}:
        Path(row['raw_path']).write_text('{"tampered":true}\n')
        if damage == 'raw_rehash': row['raw_sha256'] = raw_sha256(row['raw_path'])
    elif damage == 'path_escape': row['runtime_config_path'] = str(root.parent / 'outside.json')
    elif damage == 'path_other_case': row['runtime_config_path'] = report['rows'][2]['runtime_config_path']
    elif damage == 'plan':
        file = Path(row['runtime_config_path']).parent / 'mutation_plan.json'
        value = read(file); value['final_config']['topology_program']['actions'] = []; write(file, value)
    elif damage == 'capsule':
        value = read(row['replay_path']); value['input']['source_manifest']['head'] = 'c' * 40
        write(row['replay_path'], value)
    elif damage == 'missing_result': Path(row['runtime_result_path']).unlink()
    elif damage == 'missing_raw': Path(row['raw_path']).unlink()
    else:
        import r2f.campaign.replay as replay
        monkeypatch.setattr(replay, '_qualification', lambda *a, **kw:
                            dict(real_ros_execution=False, qualification_errors=['mock_failed_attempt']))
        if damage == 'effect_unqualified':
            row['execution_stage'] = 'effect_observed'
            value = read(row['runtime_result_path']); value['mock_ingest']['execution_stage'] = 'effect_observed'
            write(row['runtime_result_path'], value)
    result = run_audit(four_rows)
    assert not result['evidence_valid'], (damage, result)
    assert result['errors']


@pytest.mark.parametrize('claim,value', [
    ('actual_run', True), ('execution_stage', 'effect_observed'), ('execution_stage', 'judged'),
    ('raw_path', 'missing.jsonl'), ('raw_sha256', 'f' * 64), ('runtime_status', 'completed'),
    ('runtime_result_path', 'missing.json'), ('replay_path', 'missing.json'),
    ('mutation_receipt', {'hit': False}), ('observation_receipts', []), ('typed_facts', {}),
    ('precondition_receipts', []), ('oracle', {}), ('execution_qualification', {'real_ros_execution': True}),
    ('candidate_eligible', True), ('feedback_eligible', True), ('seal_eligible', True),
    ('boundary_eligible', True), ('effect_observed', True), ('judged', True),
    ('real_runtime_attempt', True), ('workspace_executed', True), ('oracle_verdict', 'unknown'),
    ('attribution', 'sut'), ('reason_code', 'topology_input_rejected:forged'),
    ('input_layer_receipts', [{'forged': True}]), ('profile_id', 'forged'), ('rmw', 'rmw_fastrtps_cpp'),
])
def test_preplan_rejection_cannot_launder_runtime_or_provenance(four_rows, claim, value):
    root, report = four_rows
    row = report['rows'][0]
    row[claim] = deepcopy(value)
    # Re-sign both report and saved stub to prove the contract does not merely
    # compare two caller-controlled labels/hashes.
    write(root / 'cases' / row['cell_id'] / 'row.json', row)
    errors = []
    assert not audit._verify_exploratory_row(root, row, report, errors), errors
    assert errors


@pytest.mark.parametrize('damage', ['checkpoint_missing', 'checkpoint_hash', 'checkpoint_source',
                                   'task_id', 'attempts', 'generation_evidence', 'deleted_evidence'])
def test_rejection_requires_model_backed_identity_not_missing_files(four_rows, damage):
    root, report = four_rows
    row = report['rows'][0]
    if damage == 'checkpoint_missing': (root / 'checkpoint.json').unlink()
    elif damage in {'checkpoint_hash', 'checkpoint_source'}:
        c = read(root / 'checkpoint.json')
        if damage == 'checkpoint_hash': c['checkpoint_sha256'] = 'f' * 64
        else:
            c['identity']['source_manifest']['head'] = 'c' * 40
            c['checkpoint_sha256'] = canonical_hash({k: v for k, v in c.items() if k != 'checkpoint_sha256'})
        write(root / 'checkpoint.json', c)
    elif damage == 'task_id': row['input_search']['task_id'] = 'f' * 64
    elif damage == 'attempts': row['topology_generation_attempts'] = []
    elif damage == 'generation_evidence':
        (root / 'cases' / row['cell_id'] / 'runtime').mkdir()
    else:
        # Turn a genuine prepared/executed cell into a stub, remove its result,
        # delete all new row markers, then re-sign. The case's capsule/config
        # and checkpoint task history still prove it is not pre-plan rejection.
        actual = report['rows'][1]
        row['cell_id'] = actual['cell_id']
        Path(actual['runtime_result_path']).unlink()
        report['rows'][1] = deepcopy(row)
    write(root / 'cases' / row['cell_id'] / 'row.json', row)
    errors = []
    assert not audit._verify_exploratory_row(root, row, report, errors), errors


def test_boundary_pending_actual_attempt_is_not_exempt(four_rows, monkeypatch):
    root, report = four_rows
    monkeypatch.setattr(audit, 'boundary_qualification_for_row', lambda *a, **kw:
                        dict(status='unknown', reason_code='mock_attempt_unqualified'))
    errors = []
    assert not audit._verify_exploratory_row(root, report['rows'][1], report, errors)
    assert 'mock_attempt_unqualified' in errors[0]


def test_configuration_digest_recomputed_not_just_preserved_bool(four_rows):
    root, report = four_rows
    row = report['rows'][1]
    cfg = read(row['runtime_config_path'])
    # Even a nonsemantic runtime-config field is in this complete-config hash.
    cfg['unexpected_override'] = True
    write(row['runtime_config_path'], cfg)
    errors = []
    assert not audit._verify_exploratory_row(root, row, report, errors)
    assert 'normalized_config_sha256' in errors[0]


def test_resigned_checkpoint_and_row_cannot_forge_model_rejection(four_rows):
    from r2f.campaign.search_frontier import InputSearchFrontier
    root, report = four_rows
    row = report['rows'][0]
    c = read(root / 'checkpoint.json')
    f = c['state']['scheduler']['frontier']
    rebuilt = InputSearchFrontier.prefix(f, 0)
    for event in deepcopy(f['journal']):
        if event['task_id'] == row['input_search']['task_id']:
            if event['action'] == 'generation_rejected': event['reason'] = 'forged model reason'
            elif event['action'] == 'finish': event['reason'] = 'topology_input_rejected:forged model reason'
        rebuilt.apply_event(event)
    c['state']['scheduler']['frontier'] = rebuilt.to_dict()
    c['checkpoint_sha256'] = canonical_hash({k: v for k, v in c.items() if k != 'checkpoint_sha256'})
    write(root / 'checkpoint.json', c)
    row['reason_code'] = row['binding']['detail'] = 'topology_input_rejected:forged model reason'
    write(root / 'cases' / row['cell_id'] / 'row.json', row)
    errors = []
    assert not audit._verify_exploratory_row(root, row, report, errors)
    assert 'topology_rejection_model_reason_mismatch' in errors[0], errors


@pytest.mark.parametrize('claim', ['hit', 'configured', 'mutation_applied', 'raw_receipt', 'eligibility'])
def test_unknown_execution_claims_on_rejection_are_fail_closed(four_rows, claim):
    root, report = four_rows
    row = report['rows'][0]
    row[claim] = True
    write(root / 'cases' / row['cell_id'] / 'row.json', row)
    errors = []
    assert not audit._verify_exploratory_row(root, row, report, errors)
    assert 'unrecognized_claim' in errors[0]


def test_rejection_does_not_enumerate_or_replay_full_frontier(four_rows, monkeypatch):
    from r2f.campaign.search_frontier import InputSearchFrontier
    root, report = four_rows
    def forbidden(*args, **kwargs):
        raise AssertionError('full frontier/model replay is not a report rejection requirement')
    monkeypatch.setattr(InputSearchFrontier, 'from_dict', forbidden)
    monkeypatch.setattr(InputSearchFrontier, 'validate_model', forbidden)
    for row in (report['rows'][0], report['rows'][3]):
        errors = []
        assert audit._verify_exploratory_row(root, row, report, errors), errors


def test_rejection_parent_generation_must_be_recomputed(four_rows):
    root, report = four_rows
    row = report['rows'][3]  # depth=2: depends on a generated parent, not a root.
    c = read(root / 'checkpoint.json')
    f = c['state']['scheduler']['frontier']
    parent = f['nodes'][row['input_search']['parent_content_sha256']]
    parent['entry']['final_config']['topology_program']['actions'] = []
    c['checkpoint_sha256'] = canonical_hash({k: v for k, v in c.items() if k != 'checkpoint_sha256'})
    write(root / 'checkpoint.json', c)
    errors = []
    assert not audit._verify_exploratory_row(root, row, report, errors)
    assert 'parent_identity_mismatch' in errors[0], errors


def test_deleted_actual_artifacts_do_not_turn_attempt_into_preplan_rejection(four_rows):
    root, report = four_rows
    actual = report['rows'][1]
    fake = deepcopy(report['rows'][0])
    fake['cell_id'] = report['run_id'] + '.0001.rejected'
    fake['input_search'] = deepcopy(actual['input_search'])
    fake['input_search'].update(status='rejected', actual=False, execution=None)
    fake['topology_generation_attempts'] = [deepcopy(fake['input_search']['mutation_request'])]
    shutil.rmtree(Path(actual['runtime_config_path']).parent)
    errors = []
    assert not audit._verify_exploratory_row(root, fake, report, errors)
    assert 'has_submission_or_plan_history' in errors[0], errors


@pytest.mark.parametrize('damage', ['checkpoint_list', 'frontier_list', 'duplicate_task', 'cyclic_parent'])
def test_malformed_resigned_checkpoint_is_invalid_not_exception(four_rows, damage):
    root, report = four_rows
    c = read(root / 'checkpoint.json')
    f = c['state']['scheduler']['frontier']
    if damage == 'checkpoint_list': c = []
    elif damage == 'frontier_list': c['state']['scheduler']['frontier'] = []
    elif damage == 'duplicate_task': f['tasks'].append(deepcopy(f['tasks'][0]))
    else:
        row = report['rows'][3]
        task = next(t for t in f['tasks'] if t['task_id'] == row['input_search']['task_id'])
        parent_hash = task['parent_content_sha256']
        proof = next(e for e in f['journal'] if e['action'] == 'plan'
                     and e['record']['content_sha256'] == parent_hash)
        prior = next(t for t in f['tasks'] if t['task_id'] == proof['task_id'])
        prior['parent_content_sha256'] = parent_hash
    if isinstance(c, dict):
        c['checkpoint_sha256'] = canonical_hash({k: v for k, v in c.items() if k != 'checkpoint_sha256'})
    write(root / 'checkpoint.json', c)
    errors = []
    row = report['rows'][3] if damage == 'cyclic_parent' else report['rows'][0]
    assert not audit._verify_exploratory_row(root, row, report, errors)
    assert errors
