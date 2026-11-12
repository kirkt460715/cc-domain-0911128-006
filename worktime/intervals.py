from datetime import datetime, timedelta, timezone
from dispatch_log.schema import integer, instant, utc


def checked(values):
    result = []
    for pair in values:
        if not isinstance(pair, (tuple, list)) or len(pair) != 2:
            raise ValueError('interval requires two endpoints')
        left, right = pair
        integer(left, 'interval.start', 0, 4102444800)
        integer(right, 'interval.end', 0, 4102444800)
        if left >= right:
            raise ValueError('interval must be positive half open range')
        result.append((left, right))
    return result


def union(values):
    result = []
    for left, right in sorted(checked(values)):
        if result and left <= result[-1][1]:
            result[-1] = result[-1][0], max(result[-1][1], right)
        else:
            result.append((left, right))
    return result


def duration(values):
    return sum(right - left for left, right in union(values))


def intersect(first, second):
    left = union(first)
    right = union(second)
    output = []
    i = 0
    j = 0
    while i < len(left) and j < len(right):
        start = max(left[i][0], right[j][0])
        end = min(left[i][1], right[j][1])
        if start < end:
            output.append((start, end))
        if left[i][1] < right[j][1]:
            i += 1
        else:
            j += 1
    return output


def subtract(first, second):
    available = union(first)
    excluded = union(second)
    result = []
    j = 0
    for left, right in available:
        current = left
        while j < len(excluded) and excluded[j][1] <= left:
            j += 1
        index = j
        while index < len(excluded) and excluded[index][0] < right:
            cut_left, cut_right = excluded[index]
            if current < cut_left:
                result.append((current, min(cut_left, right)))
            current = max(current, cut_right)
            if current >= right:
                break
            index += 1
        if current < right:
            result.append((current, right))
    return union(result)


def clip(values, start, end):
    if start >= end:
        raise ValueError('clip interval must be positive')
    return intersect(values, [(start, end)])


def split_days(values, offset_minutes=0):
    integer(offset_minutes, 'offset_minutes', -840, 840)
    offset = offset_minutes * 60
    output = []
    for left, right in union(values):
        current = left
        while current < right:
            local_day = (current + offset) // 86400
            boundary = (local_day + 1) * 86400 - offset
            end = min(right, boundary)
            day = datetime.fromtimestamp(local_day * 86400, timezone.utc).date().isoformat()
            output.append({'day': day, 'start': current, 'end': end, 'seconds': end - current, 'offset_minutes': offset_minutes})
            current = end
    return output


def boundaries(start, end, offset_minutes=0):
    integer(start, 'start', 0, 4102444800)
    integer(end, 'end', 0, 4102444800)
    if start >= end or end - start > 3660 * 86400:
        raise ValueError('report window must be positive and at most ten years')
    return split_days([(start, end)], offset_minutes)


def overlap_segments(labeled):
    events = {}
    for ident, intervals in labeled.items():
        for left, right in union(intervals):
            events.setdefault(left, []).append((1, ident))
            events.setdefault(right, []).append((-1, ident))
    active = set()
    previous = None
    output = []
    for point, changes in sorted(events.items()):
        if previous is not None and previous < point and len(active) > 1:
            output.append({'start': previous, 'end': point, 'seconds': point - previous, 'ids': sorted(active)})
        for direction, ident in sorted(changes):
            if direction == -1:
                active.remove(ident)
            else:
                active.add(ident)
        previous = point
    return output


def fair_overlap_credit(labeled):
    points = sorted({endpoint for intervals in labeled.values() for pair in union(intervals) for endpoint in pair})
    from fractions import Fraction
    credits = {ident: Fraction(0) for ident in labeled}
    rows = []
    normalized = {ident: union(values) for ident, values in labeled.items()}
    for left, right in zip(points, points[1:]):
        active = [ident for ident, values in normalized.items() if any(a <= left and right <= b for a, b in values)]
        if not active:
            continue
        share = Fraction(right - left, len(active))
        for ident in active:
            credits[ident] += share
        rows.append({'start': left, 'end': right, 'ids': sorted(active), 'credit_numerator': share.numerator, 'credit_denominator': share.denominator})
    if sum(credits.values()) != duration([pair for values in labeled.values() for pair in values]):
        raise AssertionError('overlap credit failed time conservation')
    return {'credits': {ident: {'numerator': value.numerator, 'denominator': value.denominator} for ident, value in sorted(credits.items())}, 'segments': rows, 'not_payroll_or_validity_override': True}
