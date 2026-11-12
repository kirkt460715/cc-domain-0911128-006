from collections import defaultdict
from dispatch_log.schema import instant, integer
from quality.latency import distribution


def backlog(dataset, result):
    cutoff = instant(result['effective_at'], 'effective_at')
    timelines = {row['visit']: row for row in result['timeline']['visits']}
    rows = []
    for ident, visit in sorted(dataset.visits.items()):
        timeline = timelines[ident]
        if timeline['state'] in ('completed', 'cancelled', 'not_requested'):
            continue
        requested = instant(visit['requested_at'], 'requested_at')
        promised = instant(visit['promised_start'], 'promised_start')
        service = dataset.services[visit['service']]
        response_due = requested + service['response_seconds']
        accepted = timeline['milestones'].get('accepted', [None])[0]
        started = timeline['milestones'].get('started', [None])[0]
        rows.append({
            'visit': ident,
            'client': visit['client'],
            'region': visit['region'],
            'service': visit['service'],
            'state': timeline['state'],
            'worker': timeline['worker'],
            'valid': timeline['valid'],
            'age_seconds': cutoff - requested,
            'response_overdue_seconds': max(0, cutoff - response_due) if accepted is None else 0,
            'start_overdue_seconds': max(0, cutoff - promised) if started is None else 0,
            'response_due_now': accepted is None and cutoff >= response_due,
            'start_due_now': started is None and cutoff >= promised,
            'active_so_far_seconds': timeline.get('active_seconds', 0),
            'pause_so_far_seconds': timeline.get('pause_seconds', 0),
            'invalid_state_requires_source_review': not timeline['valid'],
        })
    return {'effective_at': result['effective_at'], 'rows': rows, 'invalid_timelines_visible_but_not_counted_as_valid_demand': True}


def summarize_backlog(report, dimension='region'):
    if dimension not in ('region', 'service', 'state'):
        raise ValueError('unsupported backlog dimension')
    groups = defaultdict(list)
    for row in report['rows']:
        groups[row[dimension]].append(row)
    output = []
    for key, rows in sorted(groups.items()):
        valid = [row for row in rows if row['valid']]
        output.append({
            dimension: key,
            'valid_open_visits': len(valid),
            'invalid_open_visits': len(rows) - len(valid),
            'valid_unassigned': sum(row['worker'] is None for row in valid),
            'response_due': sum(row['response_due_now'] for row in valid),
            'start_due': sum(row['start_due_now'] for row in valid),
            'age_seconds': distribution([row['age_seconds'] for row in valid]),
            'start_overdue_seconds': distribution([row['start_overdue_seconds'] for row in valid if row['start_due_now']]),
        })
    return {'dimension': dimension, 'rows': output}


def queue_priority(report, limit=100):
    integer(limit, 'limit', 1, 1000)
    rows = [row for row in report['rows'] if row['valid']]
    rows.sort(key=lambda row: (-row['start_overdue_seconds'], -row['response_overdue_seconds'], -row['age_seconds'], row['visit']))
    return {'rows': rows[:limit], 'total_valid_open_visits': len(rows), 'policy': 'descriptive_triage_by_lateness_not_optimized_dispatch', 'does_not_assign_workers': True}


def cancellations(result):
    groups = defaultdict(list)
    for row in result['sla_visits']:
        if row['valid'] and row['state'] == 'cancelled':
            groups[(row['region'], row['cancellation_reason'])].append(row)
    output = []
    for (region, reason), rows in sorted(groups.items()):
        output.append({
            'region': region,
            'reason': reason,
            'visits': len(rows),
            'clients': len({row['client'] for row in rows}),
            'accepted_before_cancel': sum(row['accepted_at'] is not None for row in rows),
            'started_before_cancel': sum(row['started_at'] is not None for row in rows),
            'active_seconds_before_cancel': sum(row['active_seconds'] for row in rows),
            'cancelled_visits_not_counted_as_completions': True,
        })
    return output
