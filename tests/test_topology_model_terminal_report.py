"""Pure model-terminal report fixtures; no runtime, ROS or namespace is invoked."""
from copy import deepcopy
import json
from pathlib import Path
import shutil
from unittest.mock import Mock, patch

import pytest

from r2f.campaign import runner
from r2f.campaign.checkpoint import make_checkpoint_hook
from r2f.core.evidence import canonical_hash
from r2f.reports import run_report as audit
from test_model_search_topology import narrowed, fresh
from test_model_search_topology_chains import profile as chains_profile


def read(path):
    return json.loads(Path(path).read_text())


def write(path, value):
    Path(path).write_text(json.dumps(value, sort_keys=True, indent=2) + '\n')


def repo_root():
    return Path(runner.__file__).resolve().parents[3]


@pytest.fixture(scope='module')
def terminal_models(tmp_path_factory):
    root = tmp_path_factory.mktemp('model-terminal-module')
    docs = {'guard': narrowed('filter.expr', 'S1.topology.filter', {'filter_variant': 'invalid'}),
            'noop': chains_profile(2, classes={'two.same_slot'}, seed='topology.writer_only')}
    source = dict(source_repo=str(repo_root()), head='a06c42aa86cdf0a95531ca998f29f2554991772a',
                  behavior_tree_sha256='e465237dedbe8032c09168b45e532bee6bccb14a9878e53c928f36382bd3e0ff',
                  source_frozen=True, dirty_behavior_paths=[])
    for name, doc in docs.items():
        out = root / name
        runtime = Mock(side_effect=AssertionError('runtime forbidden in pure model fixture'))
        hook = make_checkpoint_hook(out, profile=doc, cycle_id='topology-authored', source_manifest=source)
        with patch.object(runner, 'source_manifest', return_value=deepcopy(source)):
            runner.run_exploratory_cycle(out, **fresh(doc), profile=doc, max_inputs=1,
                                        checkpoint_hook=hook, execute=False, runtime=runtime,
                                        run_id='terminal-fixture-' + name, runtime_boundary='isolated_netns')
        runtime.assert_not_called()
    return root


@pytest.fixture
def model_case(terminal_models, tmp_path):
    def copy(name):
        source = terminal_models / name
        root = tmp_path / name
        shutil.copytree(source, root)
        for file in root.rglob('*.json'):
            file.write_text(file.read_text().replace(str(source), str(root)))
        c = read(root / 'checkpoint.json')
        c['checkpoint_sha256'] = canonical_hash({k: v for k, v in c.items() if k != 'checkpoint_sha256'})
        write(root / 'checkpoint.json', c)
        return root, read(root / 'report.json')
    return copy


def check(root, report):
    report['report_sha256'] = audit.exploratory_report_digest(report)
    write(root / 'report.json', report)
    return audit.audit_exploratory_report(root / 'report.json')


@pytest.mark.parametrize('kind,outcome', [('guard', 'guard_terminal'), ('noop', 'no_effective_input')])
def test_generated_terminal_report_is_valid_but_never_execution(model_case, kind, outcome):
    root, report = model_case(kind)
    row = report['rows'][0]
    task = read(root / 'checkpoint.json')['state']['scheduler']['frontier']['tasks'][0]
    assert task['generation_outcome'] == outcome
    assert set(p.name for p in (root / 'cases' / row['cell_id']).iterdir()) == {'row.json', 'mutation_plan.json'}
    result = check(root, report)
    assert result['evidence_valid'], result['errors']
    assert result['rows_verified'] == 1 and result['coverage_entries'] == 0
    assert not result['sealed'] and not result['all_oracles_pass']
    assert row['actual_run'] is False and row['candidate_eligible'] is False and row['feedback_eligible'] is False
    assert read(root / 'candidate_ledger.json')['natural_candidate_count'] == 0


@pytest.mark.parametrize('damage', ['plan', 'content', 'parent', 'reason', 'noop_reason', 'source',
                                   'checkpoint_hash', 'profile', 'task', 'chain_receipt', 'prepared_duplicate'])
def test_resigned_terminal_input_or_provenance_tamper_fails(model_case, damage):
    root, report = model_case('noop' if damage in {'noop_reason', 'chain_receipt'} else 'guard')
    row = report['rows'][0]
    case = root / 'cases' / row['cell_id']
    if damage == 'plan':
        row['mutation_plan']['final_config']['topology_program']['actions'] = []
        write(case / 'mutation_plan.json', row['mutation_plan'])
    elif damage == 'content': row['content_sha256'] = 'f' * 64
    elif damage == 'parent': row['parent_content_sha256'] = 'f' * 64
    elif damage == 'reason': row['reason_code'] = row['binding']['detail'] = 'no_effective_input'
    elif damage == 'noop_reason': row['no_effective_input_reason'] = 'forged noop'
    elif damage == 'source':
        report['source_manifest']['head'] = 'f' * 40
        report['identity']['source_sha256'] = canonical_hash(report['source_manifest'])
    elif damage == 'profile': row['profile_id'] = 'forged'
    elif damage == 'task': row['input_search']['task_id'] = 'f' * 64
    elif damage == 'chain_receipt': row['topology_chain_accounting']['declared_layers'] += 1
    elif damage == 'prepared_duplicate':
        row['reason_code'] = row['binding']['detail'] = 'model_duplicate_content'
        write(case / 'runtime_config.json', {})
        write(case / 'capsule.json', {})
    else:
        c = read(root / 'checkpoint.json'); c['checkpoint_sha256'] = 'f' * 64
        write(root / 'checkpoint.json', c)
    write(case / 'row.json', row)
    result = check(root, report)
    assert not result['evidence_valid'], (damage, result)


@pytest.mark.parametrize('damage', ['actual', 'effect', 'receipt', 'raw', 'eligible', 'feedback',
                                   'submit', 'runtime', 'config', 'capsule', 'deleted_raw'])
def test_attempt_cannot_be_downgraded_to_terminal(model_case, damage):
    root, report = model_case('guard')
    row = report['rows'][0]
    case = root / 'cases' / row['cell_id']
    if damage == 'actual': row['actual_run'] = True
    elif damage == 'effect': row['execution_stage'] = 'effect_observed'
    elif damage == 'receipt': row['mutation_receipt'] = {'hit': False}
    elif damage == 'raw': row['raw_path'] = 'absent.jsonl'
    elif damage == 'eligible': row['candidate_eligible'] = True
    elif damage == 'feedback': row['feedback_eligible'] = True
    elif damage in {'config', 'capsule', 'submit'}:
        write(case / {'config': 'runtime_config.json', 'capsule': 'capsule.json', 'submit': 'submission_intent.json'}[damage], {})
    elif damage == 'runtime': (case / 'runtime').mkdir()
    else:
        c = read(root / 'checkpoint.json')
        f = c['state']['scheduler']['frontier']; task = f['tasks'][0]
        # Keep a submission in authoritative input history even though every
        # runtime/config/capsule/raw artifact is missing/deleted.
        finish = f['journal'].pop()
        f['journal'].append(dict(action='submit', task_id=task['task_id'], sequence=finish['sequence']))
        finish['sequence'] += 1; f['journal'].append(finish)
        c['checkpoint_sha256'] = canonical_hash({k: v for k, v in c.items() if k != 'checkpoint_sha256'})
        write(root / 'checkpoint.json', c)
    write(case / 'row.json', row)
    result = check(root, report)
    assert not result['evidence_valid'], (damage, result)


def test_terminal_plan_cannot_enter_strict_preplan_rejection(model_case):
    root, report = model_case('guard'); row = report['rows'][0]
    row['mutation_plan'] = None
    row['attribution'] = 'harness_input_generation'
    row['reason_code'] = row['binding']['detail'] = 'topology_input_rejected:fake'
    write(root / 'cases' / row['cell_id'] / 'row.json', row)
    assert not check(root, report)['evidence_valid']
