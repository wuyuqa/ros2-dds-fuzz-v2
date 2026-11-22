"""CP1 assessment and CP2 evaluation views of canonical review records."""
from r2f.core._json import snapshot
from r2f.core.candidate import (CAPABILITY_ASSESSMENT_SCHEMA, candidate_execution_identity,
                                validate_capability_validation)
from r2f.core.evidence import canonical_hash


def capability_assessment_projection(entry, original, *, human_confirmed):
    """Project accepted current opinions; rejected/superseded analysis stays history.

    Missing assessment on a legacy case means not assessed. Assessments may be
    imported before confirmation but never confer confirmation. Task references
    are scoped by exact case and review, so local task IDs can safely recur.
    """
    analysis = entry.get('analysis') or {}
    current = analysis.get('current')
    opinions = analysis.get('current_opinions', [current] if current else [])
    records = [record for record in opinions if record.get('capability_assessment') is not None]
    original_ref = candidate_execution_identity(original)
    candidate_id = f"{original.get('run_id')}:{original.get('cell_id')}"
    human = (entry.get('human_adjudication') or {}).get('current') or {}
    digests = {canonical_hash(record['capability_assessment']) for record in records}
    status = ('needs_evidence' if not opinions or entry.get('review_errors') else
              'conflicting' if len(digests) > 1 else
              'needs_assessment' if len(records) != len(opinions) else
              'awaiting_human_confirmation' if not human_confirmed else 'recorded')
    result = {'schema_version': CAPABILITY_ASSESSMENT_SCHEMA,
              'status': status, 'candidate_id': candidate_id, 'original_ref': original_ref,
              'human_confirmed': human_confirmed,
              'observation_mode': original.get('observation_mode', 'unknown'),
              'human_record_id': human.get('record_id') if human_confirmed else None,
              'missing_review_record_ids': sorted(record['record_id'] for record in opinions
                                                 if record.get('capability_assessment') is None),
              'reviews': [], 'tasks': []}
    for record in sorted(records, key=lambda record: record['record_id']):
        assessment = record['capability_assessment']
        refs = []
        validation = record.get('capability_validation')
        if validation is not None:
            parent = next((r for r in analysis.get('history', [])
                           if r['record_id'] == record.get('supersedes_record_id')), None)
            # History lives on the analysis projection (including superseded records).
            validation = validate_capability_validation(
                validation, record['evidence_refs'], original_ref=original_ref,
                assessment=assessment, task_review=parent)
            validation.update(review_record_id=record['record_id'],
                              review_record_sha256=record['record_sha256'])
        for proposed in assessment['proposed_tasks']:
            task_ref = {'candidate_id': candidate_id, 'review_record_id': record['record_id'],
                        'task_id': proposed['task_id']}
            refs.append(task_ref)
            result['tasks'].append({**snapshot(proposed), 'task_ref': task_ref,
                                    'candidate_id': candidate_id, 'original_ref': original_ref,
                                    'review_record_id': record['record_id'],
                                    'review_record_sha256': record['record_sha256'],
                                    'human_record_id': result['human_record_id'],
                                    'human_confirmed': human_confirmed,
                                    'assessment_status': status, 'implemented': False,
                                    'evidence_refs': {key: record['evidence_refs'][key]
                                                      for key in proposed['evidence_keys']},
                                    'evidence_sha256': {key: record['evidence_sha256'][key]
                                                        for key in proposed['evidence_keys']}})
        if validation is not None:
            for task in result['tasks']:
                if (task['review_record_id'] == record['record_id']
                        and task['task_id'] == validation['task_ref']['task_id']):
                    task['validation'] = snapshot(validation)
        result['reviews'].append({'record_id': record['record_id'],
                                  'record_sha256': record['record_sha256'],
                                  'evidence_refs': record['evidence_refs'],
                                  'evidence_sha256': record['evidence_sha256'],
                                  'assessment': assessment, 'task_refs': refs})
    return snapshot(result)
