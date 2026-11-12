from collections import defaultdict
from dispatch_log.schema import instant, digest
from worktime.calendar import shift_quality


def inspect(dataset, replay_result, attributions, conflicts, sla_rows, retry_report):
    findings = []
    for row in replay_result['visits']:
        for issue in row['issues']:
            findings.append({'severity': 'error', 'visit': row['visit'], **issue})
    for row in conflicts:
        for segment in row['conflicts']:
            findings.append({'severity': 'error', 'worker': row['worker'], 'code': 'overlapping_visit_busy_time', **segment})
    for row in attributions:
        if row['active_outside_available_shift_seconds']:
            findings.append({'severity': 'warning', 'visit': row['visit'], 'worker': row['worker'], 'code': 'active_outside_shift_or_in_break', 'seconds': row['active_outside_available_shift_seconds']})
        if row['travel_outside_available_shift_seconds']:
            findings.append({'severity': 'warning', 'visit': row['visit'], 'worker': row['worker'], 'code': 'travel_outside_shift_or_in_break', 'seconds': row['travel_outside_available_shift_seconds']})
    for row in sla_rows:
        if not row['valid']:
            continue
        for metric in ('response', 'start', 'duration'):
            if row[metric + '_denominator'] and row[metric + '_met'] is False:
                findings.append({'severity': 'warning', 'visit': row['visit'], 'code': metric + '_target_missed'})
    for issue in retry_report['issues']:
        findings.append({'severity': 'warning', **issue})
    for issue in shift_quality(dataset)['findings']:
        findings.append({'severity': 'warning', **issue})
    for event in replay_result['selection']['events']:
        latency = instant(event['observed_at'], 'observed_at') - instant(event['effective_at'], 'effective_at')
        if latency > 86400:
            findings.append({'severity': 'info', 'code': 'event_observed_after_one_day', 'event': event['id'], 'visit': event['visit'], 'seconds': latency})
    body = {'findings': findings, 'counts': {level: sum(row['severity'] == level for row in findings) for level in ('error', 'warning', 'info')}, 'invalid_timelines_excluded_from_worktime_and_sla': True}
    return {**body, 'findings_digest': digest(body)}


def issue_groups(findings, dimension='code'):
    if dimension not in ('code', 'severity', 'worker', 'visit'):
        raise ValueError('invalid issue dimension')
    groups = defaultdict(list)
    for row in findings['findings']:
        groups[row.get(dimension, 'unassigned')].append(row)
    return [{'key': key, 'count': len(values), 'visits': sorted({row['visit'] for row in values if 'visit' in row}), 'workers': sorted({row['worker'] for row in values if 'worker' in row})} for key, values in sorted(groups.items())]
