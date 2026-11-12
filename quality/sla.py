from collections import defaultdict
from dispatch_log.schema import instant
from worktime.intervals import duration, intersect


def classify(dataset, replay_result):
    cutoff = instant(replay_result['effective_at'], 'effective_at')
    output = []
    for timeline in replay_result['visits']:
        visit = dataset.visits[timeline['visit']]
        service = dataset.services[visit['service']]
        requested = instant(visit['requested_at'], 'requested_at')
        promised = instant(visit['promised_start'], 'promised_start')
        milestones = timeline['milestones']
        accepted = milestones.get('accepted', [None])[0]
        started = milestones.get('started', [None])[0]
        completed = milestones.get('completed', [None])[0]
        cancelled = milestones.get('cancelled', [None])[0]
        state = timeline['state']
        invalid = not timeline['valid']
        requested_in_scope = requested <= cutoff
        accepted_delay = accepted - requested if accepted is not None else None
        response_due = requested + service['response_seconds']
        response_known = accepted is not None or cancelled is not None or cutoff >= response_due
        response_eligible = not invalid and requested_in_scope and not (cancelled is not None and accepted is None)
        if not response_eligible or not response_known:
            response_met = None
        elif accepted is not None:
            response_met = accepted <= response_due
        else:
            response_met = False
        start_eligible = not invalid and requested_in_scope and state != 'cancelled'
        start_known = started is not None or cutoff >= promised
        start_met = None
        if start_eligible and start_known:
            start_met = started is not None and started <= promised
        active = timeline.get('active_seconds', 0)
        duration_eligible = not invalid and state == 'completed'
        duration_met = service['minimum_active_seconds'] <= active <= service['maximum_active_seconds'] if duration_eligible else None
        output.append({
            'visit': visit['id'],
            'client': visit['client'],
            'service': visit['service'],
            'region': visit['region'],
            'state': state,
            'valid': not invalid,
            'response_due': response_due,
            'promised_start': promised,
            'accepted_at': accepted,
            'started_at': started,
            'completed_at': completed,
            'cancelled_at': cancelled,
            'response_delay_seconds': accepted_delay,
            'start_delay_seconds': started - promised if started is not None else None,
            'response_denominator': response_eligible and response_known,
            'response_met': response_met,
            'start_denominator': start_eligible and start_known,
            'start_met': start_met,
            'duration_denominator': duration_eligible,
            'duration_met': duration_met,
            'active_seconds': active,
            'cancellation_reason': timeline.get('cancellation_reason'),
            'censored': timeline.get('right_censored', requested_in_scope),
            'future_request': not requested_in_scope,
            'cancellation_before_acceptance_excluded_from_response': cancelled is not None and accepted is None,
        })
    return output


def summarize(rows, dimensions=()):
    allowed = {'region', 'service', 'state'}
    if len(dimensions) != len(set(dimensions)) or set(dimensions) - allowed:
        raise ValueError('unknown or duplicate SLA dimension')
    groups = defaultdict(list)
    for row in rows:
        if not row['future_request']:
            groups[tuple(row[key] for key in dimensions)].append(row)
    output = []
    for key, visits in sorted(groups.items()):
        measures = {}
        for metric in ('response', 'start', 'duration'):
            denominator = sum(row[metric + '_denominator'] for row in visits)
            numerator = sum(row[metric + '_denominator'] and row[metric + '_met'] is True for row in visits)
            measures[metric] = {'numerator': numerator, 'denominator': denominator, 'rate': None if not denominator else {'numerator': numerator, 'denominator': denominator}}
        output.append({
            **dict(zip(dimensions, key)),
            'requested_visits': len(visits),
            'valid_visits': sum(row['valid'] for row in visits),
            'invalid_visits': sum(not row['valid'] for row in visits),
            'completed': sum(row['valid'] and row['state'] == 'completed' for row in visits),
            'cancelled': sum(row['valid'] and row['state'] == 'cancelled' for row in visits),
            'censored': sum(row['valid'] and row['censored'] for row in visits),
            'sla': measures,
        })
    return {'dimensions': list(dimensions), 'rows': output, 'cancellations_are_not_completed_services': True}
