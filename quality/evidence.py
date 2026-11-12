from collections import defaultdict
from dispatch_log.schema import instant, digest
from dispatch_log.selection import select


MILESTONES = ('accepted', 'departed', 'arrived', 'started', 'completed')


def visit_evidence(dataset, result, visit_id):
    if visit_id not in dataset.visits:
        raise KeyError(visit_id)
    cutoff = instant(result['knowledge_at'], 'knowledge_at')
    timeline = next(row for row in result['timeline']['visits'] if row['visit'] == visit_id)
    chosen = {row['id']: row['revision'] for row in result['timeline']['selection']['events']}
    events = []
    for row in sorted(dataset.events, key=lambda row: (row['id'], row['revision'])):
        if row['visit'] != visit_id:
            continue
        observed = instant(row['observed_at'], 'observed_at') <= cutoff
        selected = chosen.get(row['id']) == row['revision']
        events.append({
            'event': row['id'],
            'revision': row['revision'],
            'known_at_cutoff': observed,
            'selected_revision': selected,
            'inside_business_cutoff': instant(row['effective_at'], 'effective_at') <= instant(result['effective_at'], 'effective_at'),
            'void': row['void'],
            'event_digest': digest(row),
            'body': row if observed else None,
        })
    missing = [kind for kind in MILESTONES if kind not in timeline['milestones']]
    return {
        'visit': visit_id,
        'state': timeline['state'],
        'valid': timeline['valid'],
        'issues': timeline['issues'],
        'events': events,
        'missing_standard_milestones': missing,
        'missing_milestones_are_not_automatically_errors_for_open_or_cancelled_visits': True,
        'selection_digest': result['timeline']['selection']['selection_digest'],
    }


def revision_impact(dataset, before_knowledge, after_knowledge):
    first = instant(before_knowledge, 'before_knowledge')
    last = instant(after_knowledge, 'after_knowledge')
    if first >= last:
        raise ValueError('revision impact requires increasing knowledge window')
    old = select(dataset.events, before_knowledge)
    new = select(dataset.events, after_knowledge)
    before = {row['id']: row for row in old['events']}
    after = {row['id']: row for row in new['events']}
    rows = []
    for ident in sorted(before.keys() | after.keys()):
        left, right = before.get(ident), after.get(ident)
        if left == right:
            continue
        fields = sorted(set(left or {}) | set(right or {}))
        rows.append({
            'event': ident,
            'visit': (right or left)['visit'],
            'before_revision': left['revision'] if left else None,
            'after_revision': right['revision'] if right else None,
            'changed_fields': [key for key in fields if (left or {}).get(key) != (right or {}).get(key)],
            'before_effective_at': left['effective_at'] if left else None,
            'after_effective_at': right['effective_at'] if right else None,
        })
    return {'before_knowledge': before_knowledge, 'after_knowledge': after_knowledge, 'rows': rows, 'affected_visits': sorted({row['visit'] for row in rows}), 'removed_selection_may_mean_void_or_invalid_revision_history': True}


def completeness(result):
    groups = defaultdict(list)
    for row in result['timeline']['visits']:
        groups[row['state']].append(row)
    output = []
    for state, rows in sorted(groups.items()):
        output.append({
            'state': state,
            'visits': len(rows),
            'valid': sum(row['valid'] for row in rows),
            'invalid': sum(not row['valid'] for row in rows),
            'milestones_present': {kind: sum(kind in row['milestones'] for row in rows) for kind in MILESTONES},
            'selected_event_revisions': sum(len(row['selected_events']) for row in rows),
        })
    return {'rows': output, 'unit': 'timeline_state_at_business_and_knowledge_cutoffs'}


def timing_anomalies(dataset, result, late_seconds=86400):
    from dispatch_log.schema import integer
    integer(late_seconds, 'late_seconds', 0, 10**9)
    cutoff = instant(result['knowledge_at'], 'knowledge_at')
    rows = []
    for event in dataset.events:
        observed = instant(event['observed_at'], 'observed_at')
        if observed > cutoff:
            continue
        effective = instant(event['effective_at'], 'effective_at')
        request = instant(dataset.visits[event['visit']]['requested_at'], 'requested_at')
        codes = []
        if effective < request:
            codes.append('precedes_request')
        if observed - effective > late_seconds:
            codes.append('late_observation')
        if codes:
            rows.append({'event': event['id'], 'revision': event['revision'], 'visit': event['visit'], 'codes': codes, 'latency_seconds': observed - effective})
    return {'rows': rows, 'threshold_seconds': late_seconds, 'includes_superseded_known_revisions': True}
