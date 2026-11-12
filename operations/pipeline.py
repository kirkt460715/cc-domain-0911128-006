import json
from dispatch_log.schema import instant, digest, canonical
from dispatch_log.replay import replay
from worktime.attribution import visit_attribution, worker_conflicts, daily_worker_totals
from worktime.calendar import daily_availability
from worktime.intervals import boundaries
from quality.sla import classify, summarize
from quality.retries import chains, retry_summary
from quality.findings import inspect
from quality.waits import waiting_observations, survival


def analyze(dataset, start, end, effective_at, knowledge_at, offset_minutes=0, timeline_result=None):
    first = instant(start, 'start')
    last = instant(end, 'end')
    cutoff = instant(effective_at, 'effective_at')
    boundaries(first, last, offset_minutes)
    if last > cutoff:
        raise ValueError('worktime report end must not exceed business cutoff')
    timeline = replay(dataset, effective_at, knowledge_at) if timeline_result is None else timeline_result
    if timeline['input_digest'] != dataset.fingerprint or timeline['effective_at'] != effective_at or timeline['knowledge_at'] != knowledge_at:
        raise ValueError('timeline context mismatch')
    attributed = visit_attribution(dataset, timeline, first, last)
    conflicts = worker_conflicts(attributed)
    sla = classify(dataset, timeline)
    chains_report = chains(dataset, timeline['visits'])
    findings = inspect(dataset, timeline, attributed, conflicts, sla, chains_report)
    body = {
        'version': 1,
        'input_digest': dataset.fingerprint,
        'start': start,
        'end': end,
        'effective_at': effective_at,
        'knowledge_at': knowledge_at,
        'offset_minutes': offset_minutes,
        'timeline': timeline,
        'attributions': attributed,
        'worker_conflicts': conflicts,
        'daily_workers': daily_worker_totals(dataset, attributed, first, last, offset_minutes),
        'availability': daily_availability(dataset, first, last, offset_minutes),
        'sla_visits': sla,
        'sla_overall': summarize(sla),
        'sla_by_region': summarize(sla, ['region']),
        'retry_chains': chains_report,
        'retry_summary': retry_summary(chains_report),
        'waiting_to_start': survival(waiting_observations(dataset, timeline['visits'], effective_at)),
        'quality': findings,
        'scope': 'SLA_cohort_all_requests_through_business_cutoff_worktime_clipped_to_report_window',
    }
    body = json.loads(canonical(body))
    return {**body, 'result_digest': digest(body)}


def audit(dataset, result, replay_all=False):
    if result['input_digest'] != dataset.fingerprint:
        raise ValueError('analysis source mismatch')
    if digest({key: value for key, value in result.items() if key != 'result_digest'}) != result.get('result_digest'):
        raise ValueError('analysis checksum mismatch')
    for row in result['attributions']:
        if row['active_in_available_shift_seconds'] + row['active_outside_available_shift_seconds'] != row['active_seconds']:
            raise ValueError('active time split does not conserve')
        if row['travel_in_available_shift_seconds'] + row['travel_outside_available_shift_seconds'] != row['travel_seconds']:
            raise ValueError('travel time split does not conserve')
    for row in result['daily_workers']:
        if row['busy_in_shift_seconds'] + row['busy_outside_shift_seconds'] != row['busy_union_seconds']:
            raise ValueError('daily busy time does not conserve')
        if row['busy_in_shift_seconds'] + row['idle_available_seconds'] != row['available_seconds']:
            raise ValueError('daily available time does not conserve')
        if row['regular_busy_seconds'] + row['busy_over_daily_limit_seconds'] != row['busy_union_seconds']:
            raise ValueError('daily regular and excess time do not conserve')
    from worktime.balance import audit_daily
    audit_daily(result)
    from worktime.audit import inspect as interval_audit
    if not interval_audit(dataset, result)['valid']:
        raise ValueError('worktime interval reconstruction failed')
    from quality.metrics_audit import inspect as metrics_audit
    if not metrics_audit(dataset, result)['valid']:
        raise ValueError('SLA or quality metric reconstruction failed')
    if replay_all:
        expected = analyze(dataset, result['start'], result['end'], result['effective_at'], result['knowledge_at'], result['offset_minutes'])
        if expected != result:
            raise ValueError('full analysis replay mismatch')
    return {'valid': True, 'replay_performed': replay_all, 'valid_timelines': sum(row['valid'] for row in result['timeline']['visits']), 'invalid_timelines': sum(not row['valid'] for row in result['timeline']['visits'])}
