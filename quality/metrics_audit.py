from collections import defaultdict
from dispatch_log.schema import digest
from quality.sla import classify, summarize
from quality.retries import chains, retry_summary
from quality.waits import waiting_observations, survival


def inspect(dataset, result):
    errors = []
    visits = result['sla_visits']
    keys = [row['visit'] for row in visits]
    if len(keys) != len(set(keys)) or set(keys) != set(dataset.visits):
        errors.append({'code': 'sla_visit_key_set_mismatch'})
    for row in visits:
        for metric in ('response', 'start', 'duration'):
            denominator = row[metric + '_denominator']
            value = row[metric + '_met']
            if type(denominator) is not bool:
                errors.append({'code': 'nonboolean_denominator', 'visit': row['visit'], 'metric': metric})
            if denominator:
                if type(value) is not bool:
                    errors.append({'code': 'eligible_sla_has_unknown_outcome', 'visit': row['visit'], 'metric': metric})
                if not row['valid'] or row['future_request']:
                    errors.append({'code': 'invalid_or_future_visit_enters_sla', 'visit': row['visit'], 'metric': metric})
            elif value is not None:
                errors.append({'code': 'ineligible_sla_retains_outcome', 'visit': row['visit'], 'metric': metric})
        if row['duration_denominator'] and row['state'] != 'completed':
            errors.append({'code': 'noncompleted_duration_denominator', 'visit': row['visit']})
        if row['state'] == 'cancelled' and row['start_denominator']:
            errors.append({'code': 'cancelled_start_denominator', 'visit': row['visit']})
        if row['response_delay_seconds'] is not None and row['response_delay_seconds'] < 0 and row['valid']:
            errors.append({'code': 'negative_valid_response_delay', 'visit': row['visit']})
    expected = classify(dataset, result['timeline'])
    if expected != visits:
        errors.append({'code': 'sla_classification_reconstruction_mismatch'})
    if summarize(visits) != result['sla_overall']:
        errors.append({'code': 'sla_overall_reconstruction_mismatch'})
    if summarize(visits, ['region']) != result['sla_by_region']:
        errors.append({'code': 'sla_region_reconstruction_mismatch'})
    report = result['sla_overall']
    for row in report['rows']:
        if row['valid_visits'] + row['invalid_visits'] != row['requested_visits']:
            errors.append({'code': 'sla_requested_count_mismatch'})
        if row['completed'] + row['cancelled'] + row['censored'] != row['valid_visits']:
            errors.append({'code': 'sla_terminal_and_censored_count_mismatch'})
        for metric, count in row['sla'].items():
            if not 0 <= count['numerator'] <= count['denominator']:
                errors.append({'code': 'sla_numerator_outside_denominator', 'metric': metric})
            if not count['denominator'] and count['rate'] is not None:
                errors.append({'code': 'undefined_zero_denominator_rate', 'metric': metric})
            if count['denominator'] and count['rate'] != {'numerator': count['numerator'], 'denominator': count['denominator']}:
                errors.append({'code': 'sla_rate_components_mismatch', 'metric': metric})
    chain_report = chains(dataset, result['timeline']['visits'])
    if chain_report != result['retry_chains'] or retry_summary(chain_report) != result['retry_summary']:
        errors.append({'code': 'retry_chain_reconstruction_mismatch'})
    expected_waits = survival(waiting_observations(dataset, result['timeline']['visits'], result['effective_at']))
    if expected_waits != result['waiting_to_start']:
        errors.append({'code': 'waiting_estimate_reconstruction_mismatch'})
    findings = result['quality']
    if digest({key: value for key, value in findings.items() if key != 'findings_digest'}) != findings.get('findings_digest'):
        errors.append({'code': 'findings_checksum_mismatch'})
    for severity, count in findings['counts'].items():
        if sum(row['severity'] == severity for row in findings['findings']) != count:
            errors.append({'code': 'findings_severity_count_mismatch', 'severity': severity})
    return {'valid': not errors, 'errors': errors}


def denominator_map(result):
    groups = defaultdict(list)
    for row in result['sla_visits']:
        if not row['future_request']:
            groups[row['region']].append(row)
    output = []
    for region, rows in sorted(groups.items()):
        for metric in ('response', 'start', 'duration'):
            eligible = [row for row in rows if row[metric + '_denominator']]
            excluded = [row for row in rows if not row[metric + '_denominator']]
            output.append({
                'region': region,
                'metric': metric,
                'numerator_visits': sorted(row['visit'] for row in eligible if row[metric + '_met']),
                'denominator_visits': sorted(row['visit'] for row in eligible),
                'excluded_visits': sorted(row['visit'] for row in excluded),
                'unknown_or_ineligible_outcomes_are_not_failures': True,
                'access': 'restricted_visit_identifiers',
            })
    return output
