from collections import defaultdict
from fractions import Fraction
from dispatch_log.schema import instant, digest
from worktime.intervals import duration, union, intersect, subtract, split_days, overlap_segments, fair_overlap_credit, boundaries
from worktime.calendar import worker_availability


def visit_attribution(dataset, replay_result, start, end):
    availability = worker_availability(dataset, start, end)
    output = []
    for visit in replay_result['visits']:
        if not visit['valid'] or visit['state'] == 'not_requested':
            continue
        worker = visit['worker']
        available = availability[worker]['available_intervals'] if worker is not None else []
        active = intersect(visit['active'], [(start, end)])
        travel = intersect(visit['travel'], [(start, end)])
        pause = intersect([(row['start'], row['end']) for row in visit['pause']], [(start, end)])
        in_shift = intersect(active, available)
        out_shift = subtract(active, available)
        travel_in = intersect(travel, available)
        travel_out = subtract(travel, available)
        output.append({
            'visit': visit['visit'],
            'worker': worker,
            'state': visit['state'],
            'active': active,
            'travel': travel,
            'pause': pause,
            'active_seconds': duration(active),
            'active_in_available_shift_seconds': duration(in_shift),
            'active_outside_available_shift_seconds': duration(out_shift),
            'travel_seconds': duration(travel),
            'travel_in_available_shift_seconds': duration(travel_in),
            'travel_outside_available_shift_seconds': duration(travel_out),
            'pause_seconds': duration(pause),
            'unfinished_exposure': visit['right_censored'],
        })
    return output


def worker_conflicts(attributions):
    labeled = defaultdict(dict)
    for row in attributions:
        if row['worker'] is not None:
            labeled[row['worker']][row['visit']] = row['active'] + row['travel']
    output = []
    for worker, visits in sorted(labeled.items()):
        segments = overlap_segments(visits)
        active_seconds_sum = sum(duration(values) for values in visits.values())
        physical_seconds = duration([pair for values in visits.values() for pair in values])
        output.append({
            'worker': worker,
            'conflicts': segments,
            'conflict_seconds': duration([(row['start'], row['end']) for row in segments]),
            'sum_visit_busy_seconds': active_seconds_sum,
            'union_busy_seconds': physical_seconds,
            'duplicate_attributed_seconds': active_seconds_sum - physical_seconds,
            'visits_in_conflict': sorted({ident for row in segments for ident in row['ids']}),
            'review_credit': fair_overlap_credit(visits),
            'conflicts_are_review_flags_not_silently_accepted_productivity': True,
        })
    return output


def daily_worker_totals(dataset, attributions, start, end, offset_minutes=0):
    availability = worker_availability(dataset, start, end)
    by_worker = defaultdict(list)
    for row in attributions:
        if row['worker']:
            by_worker[row['worker']].append(row)
    output = []
    for worker in sorted(dataset.workers):
        for window in boundaries(start, end, offset_minutes):
            interval = [(window['start'], window['end'])]
            visits = by_worker[worker]
            active = intersect([pair for row in visits for pair in row['active']], interval)
            travel = intersect([pair for row in visits for pair in row['travel']], interval)
            busy = union(active + travel)
            available = intersect(availability[worker]['available_intervals'], interval)
            in_shift = intersect(busy, available)
            out_shift = subtract(busy, available)
            idle = subtract(available, busy)
            labeled = {row['visit']: intersect(row['active'] + row['travel'], interval) for row in visits}
            conflicts = overlap_segments(labeled)
            limit = dataset.workers[worker]['daily_regular_seconds']
            physical = duration(busy)
            output.append({
                'worker': worker,
                'region': dataset.workers[worker]['region'],
                'day': window['day'],
                'available_seconds': duration(available),
                'busy_union_seconds': physical,
                'active_union_seconds': duration(active),
                'travel_union_seconds': duration(travel),
                'busy_in_shift_seconds': duration(in_shift),
                'busy_outside_shift_seconds': duration(out_shift),
                'idle_available_seconds': duration(idle),
                'conflict_seconds': duration([(row['start'], row['end']) for row in conflicts]),
                'regular_busy_seconds': min(limit, physical),
                'busy_over_daily_limit_seconds': max(0, physical - limit),
                'utilization': None if not duration(available) else {'numerator': duration(in_shift), 'denominator': duration(available)},
                'offset_minutes': offset_minutes,
            })
    return output
