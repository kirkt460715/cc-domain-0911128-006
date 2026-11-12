from collections import defaultdict
from dispatch_log.schema import instant
from worktime.intervals import duration, intersect, subtract, union, boundaries, overlap_segments
from worktime.calendar import worker_availability


def visit_interval_audit(dataset, result):
    errors = []
    source = {row['visit']: row for row in result['timeline']['visits']}
    first = instant(result['start'], 'start')
    last = instant(result['end'], 'end')
    for row in result['attributions']:
        timeline = source[row['visit']]
        if not timeline['valid']:
            errors.append({'visit': row['visit'], 'code': 'invalid_timeline_attributed'})
        expected_active = intersect(timeline['active'], [(first, last)])
        expected_travel = intersect(timeline['travel'], [(first, last)])
        actual_active = union(row['active'])
        actual_travel = union(row['travel'])
        if actual_active != expected_active or actual_travel != expected_travel:
            errors.append({'visit': row['visit'], 'code': 'attributed_interval_differs_from_clipped_timeline'})
        if duration(intersect(actual_active, actual_travel)):
            errors.append({'visit': row['visit'], 'code': 'same_visit_active_travel_overlap'})
        if duration(row['active']) != row['active_seconds'] or duration(row['travel']) != row['travel_seconds']:
            errors.append({'visit': row['visit'], 'code': 'interval_duration_mismatch'})
    return {'valid': not errors, 'errors': errors}


def daily_interval_audit(dataset, result):
    first = instant(result['start'], 'start')
    last = instant(result['end'], 'end')
    available = worker_availability(dataset, first, last)
    busy = defaultdict(list)
    for row in result['attributions']:
        if row['worker'] is not None:
            busy[row['worker']].extend(row['active'] + row['travel'])
    daily = {(row['worker'], row['day']): row for row in result['daily_workers']}
    errors = []
    expected_keys = set()
    for worker in dataset.workers:
        for window in boundaries(first, last, result['offset_minutes']):
            key = worker, window['day']
            expected_keys.add(key)
            if key not in daily:
                errors.append({'worker': worker, 'day': window['day'], 'code': 'missing_worker_day'})
                continue
            actual = daily[key]
            interval = [(window['start'], window['end'])]
            occupied = intersect(busy[worker], interval)
            scheduled = intersect(available[worker]['available_intervals'], interval)
            expected = {
                'busy_union_seconds': duration(occupied),
                'available_seconds': duration(scheduled),
                'busy_in_shift_seconds': duration(intersect(occupied, scheduled)),
                'busy_outside_shift_seconds': duration(subtract(occupied, scheduled)),
                'idle_available_seconds': duration(subtract(scheduled, occupied)),
            }
            for measure, value in expected.items():
                if actual[measure] != value:
                    errors.append({'worker': worker, 'day': window['day'], 'code': 'worker_day_interval_mismatch', 'measure': measure})
    if set(daily) != expected_keys or len(daily) != len(result['daily_workers']):
        errors.append({'code': 'worker_day_key_set_mismatch'})
    return {'valid': not errors, 'errors': errors}


def inspect(dataset, result):
    visits = visit_interval_audit(dataset, result)
    daily = daily_interval_audit(dataset, result)
    return {'valid': visits['valid'] and daily['valid'], 'visit_intervals': visits, 'worker_days': daily}
