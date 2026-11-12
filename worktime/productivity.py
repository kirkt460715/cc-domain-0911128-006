from collections import defaultdict
from fractions import Fraction
from dispatch_log.schema import instant
from worktime.intervals import boundaries, intersect, union, duration, fair_overlap_credit


def exact(value):
    if not isinstance(value, dict) or set(value) != {'numerator', 'denominator'}:
        raise ValueError('expected exact rational object')
    if type(value['numerator']) is not int or type(value['denominator']) is not int or value['denominator'] <= 0:
        raise ValueError('invalid rational components')
    return Fraction(value['numerator'], value['denominator'])


def fraction(value):
    value = Fraction(value)
    return {'numerator': value.numerator, 'denominator': value.denominator}


def service_credit(dataset, result):
    first = instant(result['start'], 'start')
    last = instant(result['end'], 'end')
    by_worker = defaultdict(list)
    for row in result['attributions']:
        if row['worker'] is not None:
            by_worker[row['worker']].append(row)
    rows = []
    worker_checks = []
    for worker, attributed in sorted(by_worker.items()):
        for window in boundaries(first, last, result['offset_minutes']):
            clip = [(window['start'], window['end'])]
            labels = {}
            for row in attributed:
                active = intersect(row['active'], clip)
                travel = intersect(row['travel'], clip)
                labels[row['visit'] + ':active'] = active
                labels[row['visit'] + ':travel'] = travel
            credited = fair_overlap_credit(labels)
            groups = defaultdict(lambda: {'active': Fraction(0), 'travel': Fraction(0), 'visits': set()})
            for label, value in credited['credits'].items():
                visit_id, kind = label.rsplit(':', 1)
                service = dataset.visits[visit_id]['service']
                groups[service][kind] += exact(value)
                if exact(value):
                    groups[service]['visits'].add(visit_id)
            total = Fraction(0)
            for service, amounts in sorted(groups.items()):
                busy = amounts['active'] + amounts['travel']
                if not busy:
                    continue
                total += busy
                rows.append({
                    'worker': worker,
                    'day': window['day'],
                    'service': service,
                    'active_credit_seconds': fraction(amounts['active']),
                    'travel_credit_seconds': fraction(amounts['travel']),
                    'busy_credit_seconds': fraction(busy),
                    'distinct_visits': len(amounts['visits']),
                })
            physical = duration([pair for values in labels.values() for pair in values])
            if total != physical:
                raise AssertionError('service credit fails worker time conservation')
            worker_checks.append({'worker': worker, 'day': window['day'], 'physical_busy_seconds': physical, 'credited_seconds': fraction(total)})
    return {'rows': rows, 'worker_checks': worker_checks, 'policy': 'equal_split_each_overlapping_visit_phase', 'review_allocation_not_payroll_or_resolution_of_conflicts': True}


def service_totals(dataset, result):
    credited = service_credit(dataset, result)
    groups = defaultdict(list)
    for row in credited['rows']:
        groups[row['service']].append(row)
    rows = []
    for service, values in sorted(groups.items()):
        active = sum((exact(row['active_credit_seconds']) for row in values), Fraction(0))
        travel = sum((exact(row['travel_credit_seconds']) for row in values), Fraction(0))
        busy = active + travel
        completed = sum(row['valid'] and row['service'] == service and row['state'] == 'completed' for row in result['sla_visits'])
        rows.append({
            'service': service,
            'active_credit_seconds': fraction(active),
            'travel_credit_seconds': fraction(travel),
            'busy_credit_seconds': fraction(busy),
            'travel_share': fraction(travel / busy) if busy else None,
            'worker_days': len(values),
            'completed_in_sla_cohort': completed,
            'worktime_window_and_sla_cohort_have_different_scopes': True,
        })
    return {'rows': rows, 'rule': credited['policy']}


def daily_overtime_exposure(dataset, result):
    rows = []
    for row in result['daily_workers']:
        limit = dataset.workers[row['worker']]['daily_regular_seconds']
        available = row['available_seconds']
        busy = row['busy_union_seconds']
        rows.append({
            'worker': row['worker'],
            'day': row['day'],
            'regular_limit_seconds': limit,
            'availability_over_limit': max(0, available - limit),
            'busy_over_limit': max(0, busy - limit),
            'busy_outside_shift': row['busy_outside_shift_seconds'],
            'conflict_seconds': row['conflict_seconds'],
            'review_required': busy > limit or row['busy_outside_shift_seconds'] > 0 or row['conflict_seconds'] > 0,
            'not_a_legal_payroll_overtime_determination': True,
        })
    return rows
