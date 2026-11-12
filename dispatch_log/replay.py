import json
from collections import defaultdict
from dispatch_log.schema import instant, digest, utc, canonical
from dispatch_log.selection import select
from worktime.intervals import duration, union


TRANSITIONS = {
    'requested': {'accepted': 'accepted', 'cancelled': 'cancelled'},
    'accepted': {'departed': 'travelling', 'cancelled': 'cancelled'},
    'travelling': {'arrived': 'arrived', 'cancelled': 'cancelled'},
    'arrived': {'started': 'active', 'cancelled': 'cancelled'},
    'active': {'paused': 'paused', 'completed': 'completed', 'cancelled': 'cancelled'},
    'paused': {'resumed': 'active', 'cancelled': 'cancelled'},
    'completed': {},
    'cancelled': {},
}


def replay_visit(dataset, visit, events, effective_at, selection_issues=()):
    cutoff = instant(effective_at, 'effective_at')
    request = instant(visit['requested_at'], 'requested_at')
    if request > cutoff:
        return {'visit': visit['id'], 'state': 'not_requested', 'valid': True, 'issues': [], 'active': [], 'travel': [], 'pause': [], 'worker': None, 'milestones': {}, 'selected_events': []}
    rows = sorted([row for row in events if instant(row['effective_at'], 'effective_at') <= cutoff], key=lambda row: (row['sequence'], row['id']))
    state = 'requested'
    issues = list(selection_issues)
    worker = None
    milestones = {}
    active = []
    travel = []
    pauses = []
    active_start = None
    travel_start = None
    pause_start = None
    pause_reason = None
    previous_time = request
    for position, row in enumerate(rows, 1):
        timestamp = instant(row['effective_at'], 'event.effective_at')
        kind = row['kind']
        if row['sequence'] != position:
            issues.append({'code': 'sequence_gap_or_collision', 'event': row['id'], 'expected': position, 'actual': row['sequence']})
        if timestamp < previous_time:
            issues.append({'code': 'event_time_goes_backwards', 'event': row['id']})
        previous_time = max(previous_time, timestamp)
        if kind not in TRANSITIONS[state]:
            issues.append({'code': 'invalid_transition', 'event': row['id'], 'from': state, 'kind': kind})
            continue
        if kind == 'accepted':
            if row['worker'] is None:
                issues.append({'code': 'accepted_without_worker', 'event': row['id']})
            worker = row['worker']
        elif row['worker'] != worker:
            issues.append({'code': 'worker_changes_without_new_visit', 'event': row['id']})
        if timestamp < request:
            issues.append({'code': 'event_precedes_request', 'event': row['id']})
        if state == 'active' and active_start is not None:
            if active_start < timestamp:
                active.append((active_start, timestamp))
            active_start = None
        if state == 'travelling' and travel_start is not None:
            if travel_start < timestamp:
                travel.append((travel_start, timestamp))
            travel_start = None
        if state == 'paused' and pause_start is not None:
            if pause_start < timestamp:
                pauses.append({'start': pause_start, 'end': timestamp, 'reason': pause_reason})
            pause_start = None
        state = TRANSITIONS[state][kind]
        milestones.setdefault(kind, []).append(timestamp)
        if state == 'active':
            active_start = timestamp
        elif state == 'travelling':
            travel_start = timestamp
        elif state == 'paused':
            pause_start = timestamp
            pause_reason = row['reason']
    if active_start is not None and active_start < cutoff:
        active.append((active_start, cutoff))
    if travel_start is not None and travel_start < cutoff:
        travel.append((travel_start, cutoff))
    if pause_start is not None and pause_start < cutoff:
        pauses.append({'start': pause_start, 'end': cutoff, 'reason': pause_reason})
    cancellation = next((row['reason'] for row in reversed(rows) if row['kind'] == 'cancelled'), None)
    return {
        'visit': visit['id'],
        'state': state,
        'valid': not issues,
        'issues': issues,
        'active': union(active),
        'travel': union(travel),
        'pause': pauses,
        'worker': worker,
        'milestones': milestones,
        'selected_events': [{'id': row['id'], 'revision': row['revision'], 'sequence': row['sequence']} for row in rows],
        'active_seconds': duration(active),
        'travel_seconds': duration(travel),
        'pause_seconds': sum(row['end'] - row['start'] for row in pauses),
        'cancellation_reason': cancellation,
        'right_censored': state not in ('completed', 'cancelled'),
        'effective_at': effective_at,
    }


def replay(dataset, effective_at, knowledge_at):
    business = instant(effective_at, 'effective_at')
    knowledge = instant(knowledge_at, 'knowledge_at')
    if knowledge < business:
        raise ValueError('knowledge cutoff must not precede business cutoff')
    selected = select(dataset.events, knowledge_at)
    event_visits = defaultdict(list)
    bad_visits = defaultdict(list)
    for row in selected['events']:
        event_visits[row['visit']].append(row)
    for issue in selected['issues']:
        targets = {row['visit'] for row in dataset.events if row['id'] == issue['event']}
        for visit in targets:
            bad_visits[visit].append(issue)
    visits = [replay_visit(dataset, row, event_visits[ident], effective_at, bad_visits[ident]) for ident, row in sorted(dataset.visits.items())]
    body = {'input_digest': dataset.fingerprint, 'effective_at': effective_at, 'knowledge_at': knowledge_at, 'selection': selected, 'visits': visits}
    body = json.loads(canonical(body))
    return {**body, 'replay_digest': digest(body)}
