from collections import defaultdict
from dispatch_log.schema import instant
from worktime.intervals import split_days
from quality.sla import summarize
from quality.waits import waiting_observations, survival


def request_cohorts(dataset, result, by='day'):
    if by not in ('day', 'region', 'service'):
        raise ValueError('unsupported request cohort grouping')
    start = instant(result['start'], 'start')
    end = instant(result['end'], 'end')
    groups = defaultdict(list)
    outside = 0
    for row in result['sla_visits']:
        visit = dataset.visits[row['visit']]
        requested = instant(visit['requested_at'], 'requested_at')
        if not start <= requested < end:
            if not row['future_request']:
                outside += 1
            continue
        if row['future_request']:
            continue
        if by == 'day':
            key = split_days([(requested, requested + 1)], result['offset_minutes'])[0]['day']
        else:
            key = row[by]
        groups[key].append(row)
    output = []
    for key, rows in sorted(groups.items()):
        summary = summarize(rows)['rows'][0]
        if summary['valid_visits'] + summary['invalid_visits'] != summary['requested_visits']:
            raise AssertionError('request cohort count conservation failed')
        terminal = summary['completed'] + summary['cancelled']
        if terminal + summary['censored'] != summary['valid_visits']:
            raise AssertionError('request cohort state conservation failed')
        output.append({by: key, **summary})
    return {'by': by, 'rows': output, 'known_requests_outside_report_window': outside, 'cohort_definition': 'request_timestamp_in_half_open_worktime_report_window_followed_until_business_cutoff'}


def completion_cohorts(dataset, result):
    start = instant(result['start'], 'start')
    end = instant(result['end'], 'end')
    groups = defaultdict(list)
    for timeline in result['timeline']['visits']:
        if not timeline['valid'] or timeline['state'] != 'completed':
            continue
        completion = timeline['milestones']['completed'][0]
        if not start <= completion < end:
            continue
        day = split_days([(completion, completion + 1)], result['offset_minutes'])[0]['day']
        visit = dataset.visits[timeline['visit']]
        groups[(day, visit['service'], visit['region'])].append(timeline)
    output = []
    for (day, service, region), rows in sorted(groups.items()):
        output.append({
            'day': day,
            'service': service,
            'region': region,
            'completed_visits': len(rows),
            'clients': len({dataset.visits[row['visit']]['client'] for row in rows}),
            'whole_visit_active_seconds': sum(row['active_seconds'] for row in rows),
            'active_seconds_may_extend_outside_completion_day': True,
        })
    return {'rows': output, 'cohort_definition': 'completion_timestamp_in_half_open_report_window_not_request_or_work_day'}


def waiting_by_region(dataset, result, target='started'):
    waiting = waiting_observations(dataset, result['timeline']['visits'], result['effective_at'], target)
    groups = defaultdict(list)
    excluded = defaultdict(list)
    for row in waiting['rows']:
        groups[row['region']].append(row)
    for row in waiting['excluded']:
        region = dataset.visits[row['visit']]['region']
        excluded[region].append(row)
    output = []
    for region in sorted(groups.keys() | excluded.keys()):
        subset = {'target': target, 'rows': groups[region], 'excluded': excluded[region]}
        output.append({'region': region, 'estimate': survival(subset)})
    return {'target': target, 'rows': output, 'cancellations_excluded_as_competing_events': True}
