from collections import defaultdict
from dispatch_log.schema import digest


WORKER_MEASURES = ('available_seconds', 'busy_union_seconds', 'active_union_seconds', 'travel_union_seconds', 'busy_in_shift_seconds', 'busy_outside_shift_seconds', 'idle_available_seconds', 'conflict_seconds', 'regular_busy_seconds', 'busy_over_daily_limit_seconds')


def compare(before, after):
    for result in (before, after):
        if digest({key: value for key, value in result.items() if key != 'result_digest'}) != result.get('result_digest'):
            raise ValueError('comparison input checksum mismatch')
    for key in ('start', 'end', 'offset_minutes'):
        if before[key] != after[key]:
            raise ValueError('comparison requires matching worktime window and day offset')
    old = {row['visit']: row for row in before['sla_visits']}
    new = {row['visit']: row for row in after['sla_visits']}
    changes = []
    for ident in sorted(old.keys() | new.keys()):
        left, right = old.get(ident), new.get(ident)
        if left == right:
            continue
        fields = sorted(set(left or {}) | set(right or {}))
        values = {key: {'before': (left or {}).get(key), 'after': (right or {}).get(key)} for key in fields if (left or {}).get(key) != (right or {}).get(key)}
        changes.append({'visit': ident, 'changes': values, 'added': left is None, 'removed': right is None})
    old_workers = {(row['worker'], row['day']): row for row in before['daily_workers']}
    new_workers = {(row['worker'], row['day']): row for row in after['daily_workers']}
    workers = []
    for key in sorted(old_workers.keys() | new_workers.keys()):
        left, right = old_workers.get(key, {}), new_workers.get(key, {})
        deltas = {metric: right.get(metric, 0) - left.get(metric, 0) for metric in WORKER_MEASURES}
        if any(deltas.values()):
            workers.append({'worker': key[0], 'day': key[1], 'deltas': deltas})
    return {
        'before_digest': before['result_digest'],
        'after_digest': after['result_digest'],
        'changed_visits': changes,
        'changed_worker_days': workers,
        'knowledge_cutoff_changed': before['knowledge_at'] != after['knowledge_at'],
        'business_cutoff_changed': before['effective_at'] != after['effective_at'],
        'interpretation': 'revision_or_followup_change_not_causal_effect',
    }


def impact_by_region(before, after):
    report = compare(before, after)
    old = {row['visit']: row for row in before['sla_visits']}
    new = {row['visit']: row for row in after['sla_visits']}
    groups = defaultdict(list)
    for change in report['changed_visits']:
        ident = change['visit']
        regions = {(old.get(ident) or {}).get('region'), (new.get(ident) or {}).get('region')} - {None}
        for region in regions:
            groups[region].append(ident)
    rows = []
    for region, visits in sorted(groups.items()):
        metrics = {}
        for kind in ('response', 'start', 'duration'):
            old_rows = [row for row in before['sla_visits'] if row['region'] == region]
            new_rows = [row for row in after['sla_visits'] if row['region'] == region]
            metrics[kind] = {
                'denominator_change': sum(row[kind + '_denominator'] for row in new_rows) - sum(row[kind + '_denominator'] for row in old_rows),
                'numerator_change': sum(row[kind + '_denominator'] and row[kind + '_met'] is True for row in new_rows) - sum(row[kind + '_denominator'] and row[kind + '_met'] is True for row in old_rows),
            }
        rows.append({'region': region, 'changed_visits': sorted(visits), 'sla_deltas': metrics})
    return rows
