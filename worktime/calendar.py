from collections import defaultdict
from dispatch_log.schema import instant, integer, digest
from worktime.intervals import union, subtract, intersect, duration, split_days, boundaries, overlap_segments


def worker_availability(dataset, start, end):
    if not start < end:
        raise ValueError('availability range must be positive')
    available = defaultdict(list)
    breaks = defaultdict(list)
    raw_shifts = defaultdict(dict)
    for ident, shift in dataset.shifts.items():
        left = max(start, instant(shift['start'], 'shift.start'))
        right = min(end, instant(shift['end'], 'shift.end'))
        if left >= right:
            continue
        available[shift['worker']].append((left, right))
        raw_shifts[shift['worker']][ident] = [(left, right)]
        for pause in shift['breaks']:
            first = max(left, instant(pause['start'], 'break.start'))
            last = min(right, instant(pause['end'], 'break.end'))
            if first < last:
                breaks[shift['worker']].append((first, last))
    output = {}
    for worker in sorted(dataset.workers):
        shifts = union(available[worker])
        paused = intersect(shifts, breaks[worker])
        service = subtract(shifts, paused)
        output[worker] = {
            'worker': worker,
            'shift_intervals': shifts,
            'break_intervals': paused,
            'available_intervals': service,
            'shift_seconds': duration(shifts),
            'break_seconds': duration(paused),
            'available_seconds': duration(service),
            'overlapping_shift_records': overlap_segments(raw_shifts[worker]),
            'break_rule': 'union_all_declared_breaks_then_subtract_from_union_shifts',
        }
    return output


def daily_availability(dataset, start, end, offset_minutes=0):
    integer(offset_minutes, 'offset_minutes', -840, 840)
    available = worker_availability(dataset, start, end)
    days = boundaries(start, end, offset_minutes)
    output = []
    for worker, row in sorted(available.items()):
        for day in days:
            window = [(day['start'], day['end'])]
            shifts = duration(intersect(row['shift_intervals'], window))
            pauses = duration(intersect(row['break_intervals'], window))
            service = duration(intersect(row['available_intervals'], window))
            limit = dataset.workers[worker]['daily_regular_seconds']
            output.append({
                'worker': worker,
                'region': dataset.workers[worker]['region'],
                'day': day['day'],
                'window_seconds': day['seconds'],
                'shift_seconds': shifts,
                'break_seconds': pauses,
                'available_seconds': service,
                'regular_availability_seconds': min(limit, service),
                'availability_over_daily_limit_seconds': max(0, service - limit),
                'offset_minutes': offset_minutes,
            })
    return output


def shift_quality(dataset):
    findings = []
    by_worker = defaultdict(dict)
    for ident, shift in dataset.shifts.items():
        left = instant(shift['start'], 'start')
        right = instant(shift['end'], 'end')
        by_worker[shift['worker']][ident] = [(left, right)]
        break_values = [(instant(row['start'], 'start'), instant(row['end'], 'end')) for row in shift['breaks']]
        raw = sum(b - a for a, b in break_values)
        merged = duration(break_values)
        if raw > merged:
            findings.append({'code': 'overlapping_break_records', 'shift': ident, 'duplicate_seconds': raw - merged})
    for worker, shifts in sorted(by_worker.items()):
        for conflict in overlap_segments(shifts):
            findings.append({'code': 'overlapping_shift_records', 'worker': worker, **conflict})
    return {'findings': findings, 'union_semantics_prevent_double_counting': True}
