"""Public input/recipe contracts only: no runtime, ROS, replay or qualification."""
from copy import deepcopy
import json
from pathlib import Path

import pytest

from r2f.campaign.corpus import content_identity, corpus_from_profile
from r2f.campaign.search_frontier import derived_record
from r2f.campaign.tool_failures import generation_recipe, validate_recipe
from r2f.core import Registry, canonical_hash
from r2f.modules.s1.module import register_exploratory
from r2f.modules.s1.topology_input import make_topology_plan

SOURCE = {'source_repo': '/offline/derived-recipe-fixture', 'head': 'a' * 40,
          'behavior_tree_sha256': 'b' * 64, 'source_frozen': False}


@pytest.fixture(scope='module')
def topology():
    path = Path(__file__).resolve().parents[1] / 'config/s1_fuzz/profiles/model-bfs-topology.json'
    profile = json.loads(path.read_text())
    profile['rmws'] = profile['rmws'][:1]
    specs = [s for s in profile['seed_specs'] if s['seed_id'] == 'topology.multi_endpoint']
    profile['seed_specs'] = deepcopy(specs)
    profile['declared_seed_specs_union'] = deepcopy(specs)
    profile['cycles'][0]['seed_specs'] = deepcopy(specs)
    root = corpus_from_profile(profile, cycle_id='topology-authored').selectable()[0]
    registry = Registry()
    register_exploratory(registry, include_required=False, runtime_capability='topology_v1')
    return root, registry.get_target(root['target_id']), profile


def plan_for(entry, operator='action.insert', seed=7):
    request = {'operator_id': operator}
    if operator == 'queue.depth':
        request.update(slot_id='S1.topology.qos.depth', context={'boundary_class': 'neighbor'})
    return make_topology_plan(entry['final_config']['topology_program'], [request], rng_seed=seed)


def recipe_for(entry, plan, target, profile, operator_class='insert-action'):
    return generation_recipe(entry, target, plan, source=SOURCE, carrier='ros2_endpoint',
        mutation_kind='endpoint_program', runtime_boundary='host', profile=profile,
        sampling_kind=None, selection={'class_id': operator_class})


@pytest.mark.parametrize('parameter_class', [None, 'declared-topology-parameters'])
def test_deep_descendants_keep_declared_metadata_and_actual_operator_recipe(topology, parameter_class):
    root, target, profile = topology
    parent = deepcopy(root)
    if parameter_class is not None:
        parent['parameter_class'] = parameter_class
    expected_parameter = parameter_class or root['boundary_class']
    recipes, contents = {}, set()
    for depth in range(1, 7):
        operator = 'queue.depth' if depth % 2 else 'action.insert'
        operator_class = 'neighbor' if operator == 'queue.depth' else 'insert-action'
        before = deepcopy(parent)
        plan = plan_for(parent, operator, seed=7 + depth)
        recipe = recipe_for(parent, plan, target, profile, operator_class)
        key = validate_recipe(recipe)
        assert recipe['boundary_class'] == root['boundary_class']
        assert recipe['parameter_class'] == expected_parameter
        assert recipe['ordered_route'][0]['operator_id'] == operator
        assert recipe['ordered_route'][0]['parameter_class'] == operator_class
        assert recipe['source'] == {k: SOURCE[k] for k in ('source_repo', 'head', 'behavior_tree_sha256')}
        assert recipe['target'] == dict(target_id=target.target_id, target_revision=target.target_revision,
            contract_hash=plan.contract_hash, target_sha256=canonical_hash(target.to_dict()),
            rule_sha256=canonical_hash(target.rule_contract))
        # Sampled programs/RNG/lineage differ; only declared recipe axes group.
        if operator in recipes:
            assert (recipe, key) == recipes[operator]
        recipes[operator] = (recipe, key)
        child = derived_record(parent, plan)
        assert parent == before
        assert child['parent_content_sha256'] == parent['content_sha256']
        assert child['content_sha256'] == content_identity(child)
        assert child['boundary_class'] == root['boundary_class']
        if parameter_class is None:
            assert 'parameter_class' not in child
        else:
            assert child['parameter_class'] == parameter_class
        contents.add(child['content_sha256'])
        parent = child
    assert len(contents) >= 3


@pytest.mark.parametrize('missing', ['both', 'boundary_only', 'parameter_only'])
def test_absent_metadata_has_only_existing_boundary_fallback(topology, missing):
    root, target, profile = topology
    parent = deepcopy(root)
    if missing in {'both', 'boundary_only'}:
        parent.pop('boundary_class')
    if missing == 'boundary_only':
        parent['parameter_class'] = 'declared-topology-parameters'
    child = derived_record(parent, plan_for(parent))
    assert ('boundary_class' in child) == ('boundary_class' in parent)
    assert ('parameter_class' in child) == ('parameter_class' in parent)
    recipe = recipe_for(child, plan_for(child), target, profile)
    if missing == 'parameter_only':
        assert recipe['parameter_class'] == parent['boundary_class']
        validate_recipe(recipe)
    else:
        with pytest.raises(ValueError, match='recipe requires (parameter_class|boundary_class)'):
            validate_recipe(recipe)


@pytest.mark.parametrize('field', ['boundary_class', 'parameter_class'])
@pytest.mark.parametrize('invalid', [None, '', '   ', False, 7, {'unexpected': 'class'}])
def test_invalid_metadata_is_preserved_without_defaulting_or_recipe_exemption(topology, field, invalid):
    root, target, profile = topology
    parent = deepcopy(root)
    parent[field] = deepcopy(invalid)
    # No valid fallback for a malformed parameter class; a valid explicit
    # parameter class isolates boundary validation in the other direction.
    parent['boundary_class' if field == 'parameter_class' else 'parameter_class'] = (
        None if field == 'parameter_class' else 'declared-topology-parameters')
    child = derived_record(parent, plan_for(parent))
    assert canonical_hash(child[field]) == canonical_hash(invalid)
    if isinstance(invalid, dict):
        child[field]['unexpected'] = 'detached-copy'
        assert parent[field] == invalid
        child[field] = deepcopy(invalid)
    recipe = recipe_for(child, plan_for(child), target, profile)
    with pytest.raises(ValueError, match='recipe requires (parameter_class|boundary_class)'):
        validate_recipe(recipe)


def test_descendant_recipe_retains_source_and_target_partitions(topology):
    root, target, profile = topology
    child = derived_record(root, plan_for(root))
    recipe = recipe_for(child, plan_for(child), target, profile)
    key = validate_recipe(recipe)
    for section, field, value in [('source', 'head', 'c' * 40),
                                  ('source', 'behavior_tree_sha256', 'd' * 64),
                                  ('target', 'target_revision', target.target_revision + 1),
                                  ('target', 'target_sha256', 'e' * 64)]:
        other = deepcopy(recipe)
        other[section][field] = value
        assert validate_recipe(other) != key
    for section, field, value, reason in [('source', 'head', None, 'source partition'),
                                         ('target', 'target_revision', True, 'target contract partition')]:
        other = deepcopy(recipe)
        other[section][field] = value
        with pytest.raises(ValueError, match=reason):
            validate_recipe(other)
