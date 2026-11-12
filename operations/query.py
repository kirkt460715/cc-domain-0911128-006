import base64
import json
from dispatch_log.schema import canonical, digest, integer
from quality.sla import summarize
from quality.findings import issue_groups
from quality.waits import waiting_observations, survival, restricted_mean
from worktime.calendar import shift_quality


FILTERS = {'visit', 'client', 'region', 'service', 'state', 'valid'}
FIELDS = FILTERS | {'response_due', 'promised_start', 'accepted_at', 'started_at', 'completed_at', 'cancelled_at', 'response_delay_seconds', 'start_delay_seconds', 'response_denominator', 'response_met', 'start_denominator', 'start_met', 'duration_denominator', 'duration_met', 'active_seconds', 'cancellation_reason', 'censored', 'future_request', 'cancellation_before_acceptance_excluded_from_response'}


def encode(value):
    return base64.urlsafe_b64encode(canonical(value).encode()).decode().rstrip('=')


def decode(value):
    if not isinstance(value, str) or len(value) > 10000:
        raise ValueError('invalid query cursor')
    try:
        output = json.loads(base64.b64decode(value + '=' * (-len(value) % 4), altchars=b'-_', validate=True))
    except (ValueError, UnicodeError) as exc:
        raise ValueError('malformed query cursor') from exc
    if not isinstance(output, dict):
        raise ValueError('cursor must be object')
    return output


def visits(result, filters=None, columns=None, limit=100, cursor=None):
    filters = {} if filters is None else filters
    columns = sorted(FIELDS) if columns is None else columns
    if not isinstance(filters, dict) or filters.keys() - FILTERS:
        raise ValueError('unsupported visit filter')
    for values in filters.values():
        if not isinstance(values, list) or any(type(v) not in (str, bool) for v in values):
            raise ValueError('filters require string or boolean lists')
        if len(values) != len(set((type(v).__name__, v) for v in values)):
            raise ValueError('duplicate filter value')
    if not isinstance(columns, list) or not columns or len(columns) != len(set(columns)) or set(columns) - FIELDS:
        raise ValueError('invalid visit projection')
    integer(limit, 'limit', 1, 1000)
    rows = sorted([row for row in result['sla_visits'] if all(any(type(row[key]) is type(value) and row[key] == value for value in values) for key, values in filters.items())], key=lambda row: row['visit'])
    context = digest({'result': result['result_digest'], 'filters': filters, 'columns': columns})
    offset = 0
    if cursor is not None:
        parsed = decode(cursor)
        if set(parsed) != {'context', 'offset'} or parsed['context'] != context:
            raise ValueError('cursor belongs to another result or query')
        offset = integer(parsed['offset'], 'offset', 0, len(rows))
    page = rows[offset:offset + limit]
    next_offset = offset + len(page)
    return {'total': len(rows), 'rows': [{key: row[key] for key in columns} for row in page], 'next_cursor': encode({'context': context, 'offset': next_offset}) if next_offset < len(rows) else None, 'access': 'restricted_visit_data'}


def detail(dataset, result, visit_id):
    if visit_id not in dataset.visits:
        raise KeyError(visit_id)
    timeline = next(row for row in result['timeline']['visits'] if row['visit'] == visit_id)
    sla = next(row for row in result['sla_visits'] if row['visit'] == visit_id)
    attributed = next((row for row in result['attributions'] if row['visit'] == visit_id), None)
    findings = [row for row in result['quality']['findings'] if row.get('visit') == visit_id]
    issues = [row for row in result['worker_conflicts'] if visit_id in row['visits_in_conflict']]
    return {'visit': dataset.visits[visit_id], 'timeline': timeline, 'sla': sla, 'worktime': attributed, 'findings': findings, 'worker_conflicts': issues, 'access': 'restricted_visit_and_worker_data'}


def report(dataset, result, kind, options=None):
    from dispatch_log.schema import fields
    options = {} if options is None else options
    if kind == 'denominators':
        from quality.metrics_audit import denominator_map
        fields(options, ())
        return denominator_map(result)
    if kind == 'metrics-audit':
        from quality.metrics_audit import inspect
        fields(options, ())
        return inspect(dataset, result)
    if kind == 'interval-audit':
        from worktime.audit import inspect
        fields(options, ())
        return inspect(dataset, result)
    if kind == 'evidence':
        from quality.evidence import visit_evidence
        fields(options, ('visit',))
        return visit_evidence(dataset, result, options['visit'])
    if kind == 'revision-impact':
        from quality.evidence import revision_impact
        fields(options, ('before', 'after'))
        return revision_impact(dataset, options['before'], options['after'])
    if kind == 'completeness':
        from quality.evidence import completeness
        fields(options, ())
        return completeness(result)
    if kind == 'timing-anomalies':
        from quality.evidence import timing_anomalies
        fields(options, (), ('late_seconds',))
        return timing_anomalies(dataset, result, options.get('late_seconds', 86400))
    if kind == 'csv':
        from operations.csv_reports import export
        fields(options, ('kind',))
        return export(result, options['kind'])
    if kind == 'phase-balance':
        from worktime.balance import phase_balance
        fields(options, ())
        return phase_balance(dataset, result)
    if kind in ('request-cohorts', 'completion-cohorts', 'waiting-by-region'):
        from quality.cohorts import request_cohorts, completion_cohorts, waiting_by_region
        if kind == 'request-cohorts':
            fields(options, (), ('by',))
            return request_cohorts(dataset, result, options.get('by', 'day'))
        if kind == 'waiting-by-region':
            fields(options, (), ('target',))
            return waiting_by_region(dataset, result, options.get('target', 'started'))
        fields(options, ())
        return completion_cohorts(dataset, result)
    if kind == 'visits':
        fields(options, (), ('filters', 'columns', 'limit', 'cursor'))
        return visits(result, **options)
    if kind == 'visit':
        fields(options, ('id',))
        return detail(dataset, result, options['id'])
    if kind == 'sla':
        fields(options, (), ('dimensions',))
        return summarize(result['sla_visits'], options.get('dimensions', []))
    if kind == 'issues':
        fields(options, (), ('dimension',))
        return issue_groups(result['quality'], options.get('dimension', 'code'))
    if kind == 'waits':
        fields(options, (), ('target', 'horizon_seconds'))
        waiting = waiting_observations(dataset, result['timeline']['visits'], result['effective_at'], options.get('target', 'started'))
        if 'horizon_seconds' in options:
            return restricted_mean(waiting, options['horizon_seconds'])
        return survival(waiting)
    if kind == 'shifts':
        fields(options, ())
        return shift_quality(dataset)
    if kind == 'public':
        from operations.publication import safe_sla
        fields(options, (), ('dimension', 'minimum_clients'))
        return safe_sla(result, **options)
    if kind == 'latency':
        from quality.latency import event_latency
        fields(options, ())
        return event_latency(dataset, result['knowledge_at'])
    if kind == 'revision-churn':
        from quality.latency import revision_churn
        fields(options, ())
        return revision_churn(dataset, result['knowledge_at'])
    if kind == 'latency-buckets':
        from quality.latency import lateness_buckets
        fields(options, (), ('thresholds',))
        return lateness_buckets(dataset, result['knowledge_at'], options.get('thresholds'))
    if kind in ('service-credit', 'service-totals', 'overtime'):
        from worktime.productivity import service_credit, service_totals, daily_overtime_exposure
        fields(options, ())
        return {'service-credit': service_credit, 'service-totals': service_totals, 'overtime': daily_overtime_exposure}[kind](dataset, result)
    if kind in ('backlog', 'backlog-summary', 'triage', 'cancellations'):
        from quality.backlog import backlog, summarize_backlog, queue_priority, cancellations
        if kind == 'cancellations':
            fields(options, ())
            return cancellations(result)
        queue = backlog(dataset, result)
        if kind == 'backlog':
            fields(options, ())
            return queue
        if kind == 'backlog-summary':
            fields(options, (), ('dimension',))
            return summarize_backlog(queue, options.get('dimension', 'region'))
        fields(options, (), ('limit',))
        return queue_priority(queue, options.get('limit', 100))
    simple = {'daily-workers': 'daily_workers', 'conflicts': 'worker_conflicts', 'availability': 'availability', 'retry-chains': 'retry_chains', 'retry-summary': 'retry_summary', 'quality': 'quality'}
    if kind in simple:
        fields(options, ())
        return result[simple[kind]]
    raise ValueError('unknown report kind')
