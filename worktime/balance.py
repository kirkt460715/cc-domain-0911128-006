from collections import defaultdict
from dispatch_log.schema import instant
from worktime.intervals import boundaries, intersect, subtract, union, duration
from worktime.calendar import worker_availability


def phase_balance(dataset, result):
    first = instant(result['start'], 'start')
    last = instant(result['end'], 'end')
    shifts = worker_availability(dataset, first, last)
    groups = defaultdict(list)
    for row in result['attributions']:
        if row['worker'] is not None:
            groups[row['worker']].append(row)
    rows = []
    for worker in sorted(dataset.workers):
        active = union([pair for row in groups[worker] for pair in row['active']])
        travel = union([pair for row in groups[worker] for pair in row['travel']])
        overlap = intersect(active, travel)
        active_only = subtract(active, travel)
        travel_only = subtract(travel, active)
        for window in boundaries(first, last, result['offset_minutes']):
            interval = [(window['start'], window['end'])]
            active_part = duration(intersect(active_only, interval))
            travel_part = duration(intersect(travel_only, interval))
            conflict_part = duration(intersect(overlap, interval))
            busy = duration(intersect(active + travel, interval))
            if active_part + travel_part + conflict_part != busy:
                raise AssertionError('active/travel phase partition does not conserve')
            break_busy = duration(intersect(active + travel, intersect(shifts[worker]['break_intervals'], interval)))
            outside_shift = duration(subtract(intersect(active + travel, interval), shifts[worker]['shift_intervals']))
            rows.append({
                'worker': worker,
                'day': window['day'],
                'active_without_travel_seconds': active_part,
                'travel_without_active_seconds': travel_part,
                'active_and_travel_overlap_seconds': conflict_part,
                'busy_union_seconds': busy,
                'busy_in_declared_break_seconds': break_busy,
                'busy_outside_any_shift_seconds': outside_shift,
                'phase_overlap_can_arise_from_different_visits': True,
            })
    return rows


def audit_daily(result):
    by_worker = defaultdict(list)
    for row in result['daily_workers']:
        by_worker[row['worker']].append(row)
    rows = []
    for worker, daily in sorted(by_worker.items()):
        keys = [row['day'] for row in daily]
        if len(keys) != len(set(keys)):
            raise ValueError('duplicate worker day')
        for row in daily:
            if any(row[key] < 0 for key in ('available_seconds', 'busy_union_seconds', 'busy_in_shift_seconds', 'busy_outside_shift_seconds', 'idle_available_seconds', 'conflict_seconds')):
                raise ValueError('negative worker day measure')
            if row['busy_in_shift_seconds'] > row['available_seconds']:
                raise ValueError('busy intersection exceeds available shift')
            if row['conflict_seconds'] > row['busy_union_seconds']:
                raise ValueError('conflict exposure exceeds busy union')
        rows.append({'worker': worker, 'days': len(daily), 'busy_seconds': sum(row['busy_union_seconds'] for row in daily), 'conflict_seconds': sum(row['conflict_seconds'] for row in daily)})
    return {'valid': True, 'workers': rows}
