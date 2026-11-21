"""Pure projection of existing evidence-bound lifecycle/review ledger entries."""
from r2f.core._json import snapshot
from r2f.core.candidate import candidate_execution_identity
from r2f.core.evidence import canonical_hash

ACCOUNTING_SCHEMA = 'r2f.s1-root-cause-accounting/v1'


def current_review_projection(opinions):
    """Keep every current opinion; a representative exists only on agreement."""
    ordered = sorted(opinions, key=lambda record: record['record_id'])
    judgments = {canonical_hash({
        'attribution': record.get('attribution'), 'conclusion': record.get('conclusion'),
        'root': (record.get('root_cause') or {}).get('id'),
        'novelty': (record.get('root_cause') or {}).get('novelty', 'unknown'),
        'tool_repair_handoff': record.get('tool_repair_handoff')}) for record in ordered}
    projection = {'current': snapshot(ordered[0]) if len(judgments) == 1 else None}
    if len(ordered) > 1:
        projection['current_opinions'] = snapshot(ordered)
    return projection


def root_cause_accounting(entries, originals):
    """Count cases separately from roots; analysis never grants human authority."""
    cases, grouped, handoffs, root_evidence = [], {}, [], {}
    attributions = {key: [] for key in ('sut_candidate', 'tool', 'environment', 'allowed', 'unknown')}
    for entry in entries:
        projection = entry.get('analysis') or {}
        analysis = projection.get('current') or {}
        opinions = projection.get('current_opinions', [analysis] if analysis else [])
        supporting = [record for record in opinions
                      if record.get('attribution') == 'sut_candidate'
                      and record.get('conclusion') == 'supported']
        roots = {(record.get('root_cause') or {}).get('id') for record in supporting}
        novelties = {(record.get('root_cause') or {}).get('novelty', 'unknown')
                     for record in supporting}
        human = (entry.get('human_adjudication') or {}).get('current') or {}
        qualified = (entry.get('candidate_status') == 'confirmed'
                     and entry.get('responsibility_layer') == 'sut'
                     and entry.get('triage_disposition') == 'candidate'
                     and not entry.get('admission_error')
                     and not entry.get('human_qualification_error')
                     and bool(supporting) and len(supporting) == len(opinions)
                     and human.get('action') == 'confirm')
        root = next(iter(roots)) if len(roots) == 1 else None
        novelty = next(iter(novelties)) if root and len(novelties) == 1 else 'unknown'
        case = {'candidate_id': (opinions[0].get('candidate_id') if opinions else None) or
                f"{entry.get('run_id')}:{entry.get('cell_id')}",
                'review_record_id': analysis.get('record_id'),
                'review_record_ids': sorted(record['record_id'] for record in supporting),
                'human_confirmed': qualified, 'root_cause_id': root,
                'novelty': novelty}
        cases.append(case)
        attributes = {record.get('attribution', 'unknown') for record in opinions}
        attribution = (next(iter(attributes)) if len(attributes) == 1 else None) or (
            'tool' if entry.get('reason_code') == 'tool_error' else 'unknown')
        attributions[attribution].append(case['candidate_id'])
        if attribution == 'tool':
            handoff = analysis.get('tool_repair_handoff')
            handoffs.append({'candidate_id': case['candidate_id'],
                             'review_record_id': analysis.get('record_id'),
                             'original_ref': candidate_execution_identity(originals[case['candidate_id']]),
                             'raw_sha256': entry['original_result']['raw_sha256'],
                             'status': 'ready' if handoff else 'needs_evidence',
                             'evidence_refs': analysis.get('evidence_refs', {}),
                             'evidence_sha256': analysis.get('evidence_sha256', {}),
                             **(handoff or {})})
        # Analysis constrains novelty before human confirmation. It never creates
        # a confirmed group on its own, and rejected/invalid history has no heads.
        for record in supporting:
            evidence_cause = record.get('root_cause') or {}
            evidence_root = evidence_cause.get('id')
            if evidence_root:
                root_evidence.setdefault(evidence_root, []).append({
                    'candidate_id': case['candidate_id'], 'record_id': record['record_id'],
                    'novelty': evidence_cause.get('novelty', 'unknown'),
                    'disputed': len(novelties) != 1})
        if qualified and root:
            grouped.setdefault(root, []).append(case)
    groups = []
    for root, members in sorted(grouped.items()):
        evidence = root_evidence[root]
        novelties = {record['novelty'] for record in evidence}
        novelty = ('unknown' if any(record['disputed'] for record in evidence) else
                   'historical' if 'historical' in novelties else
                   'new' if novelties == {'new'} else 'unknown')
        groups.append({'root_cause_id': root, 'novelty': novelty,
                       'candidate_ids': sorted(case['candidate_id'] for case in members),
                       'evidence_candidate_ids': sorted({r['candidate_id'] for r in evidence}),
                       'evidence_review_record_ids': sorted(r['record_id'] for r in evidence),
                       'review_record_ids': sorted(record_id for case in members
                                                   for record_id in case['review_record_ids'])})
    return snapshot({'schema_version': ACCOUNTING_SCHEMA,
                     'human_confirmed_cases': sum(case['human_confirmed'] for case in cases),
                     'confirmed_root_causes': len(groups),
                     'independent_new_root_causes': sum(g['novelty'] == 'new' for g in groups),
                     'historical_root_causes': sum(g['novelty'] == 'historical' for g in groups),
                     'unknown_novelty_root_causes': sum(g['novelty'] == 'unknown' for g in groups),
                     'unresolved_root_cause_cases': sum(c['human_confirmed'] and not c['root_cause_id'] for c in cases),
                     'attributions': {key: sorted(ids) for key, ids in attributions.items()},
                     'tool_repair_handoffs': sorted(handoffs, key=lambda h: h['candidate_id']),
                     'groups': groups, 'cases': sorted(cases, key=lambda c: c['candidate_id'])})
