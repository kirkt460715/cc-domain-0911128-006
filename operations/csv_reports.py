import csv
import io
from dispatch_log.schema import canonical


SCHEMAS = {
    'daily-workers': ['worker', 'region', 'day', 'available_seconds', 'busy_union_seconds', 'active_union_seconds', 'travel_union_seconds', 'busy_in_shift_seconds', 'busy_outside_shift_seconds', 'idle_available_seconds', 'conflict_seconds', 'regular_busy_seconds', 'busy_over_daily_limit_seconds', 'utilization', 'offset_minutes'],
    'availability': ['worker', 'region', 'day', 'window_seconds', 'shift_seconds', 'break_seconds', 'available_seconds', 'regular_availability_seconds', 'availability_over_daily_limit_seconds', 'offset_minutes'],
    'sla-visits': ['visit', 'client', 'service', 'region', 'state', 'valid', 'response_due', 'promised_start', 'accepted_at', 'started_at', 'completed_at', 'cancelled_at', 'response_delay_seconds', 'start_delay_seconds', 'response_denominator', 'response_met', 'start_denominator', 'start_met', 'duration_denominator', 'duration_met', 'active_seconds', 'cancellation_reason', 'censored', 'future_request', 'cancellation_before_acceptance_excluded_from_response'],
}


def safe_cell(value):
    if value is None:
        return ''
    if type(value) is bool:
        return 'true' if value else 'false'
    if isinstance(value, (dict, list)):
        return canonical(value)
    if isinstance(value, str) and value.lstrip().startswith(('=', '+', '-', '@', '\t', '\r')):
        return "'" + value
    return value


def export(result, kind):
    if kind not in SCHEMAS:
        raise ValueError('unsupported CSV report kind')
    rows = result[kind.replace('-', '_')]
    columns = SCHEMAS[kind]
    output = io.StringIO(newline='')
    writer = csv.writer(output, lineterminator='\n')
    writer.writerow(columns)
    for row in rows:
        if set(row) != set(columns):
            raise ValueError('CSV report row schema mismatch')
        writer.writerow([safe_cell(row[key]) for key in columns])
    return {'result_digest': result['result_digest'], 'kind': kind, 'rows': len(rows), 'content': output.getvalue(), 'access': 'restricted_contains_worker_or_client_data', 'formula_cells_escaped': True}


def parse_export(report):
    kind = report.get('kind')
    if kind not in SCHEMAS:
        raise ValueError('unknown exported report')
    reader = csv.reader(io.StringIO(report['content']), strict=True)
    columns = next(reader, None)
    if columns != SCHEMAS[kind]:
        raise ValueError('exported CSV column order mismatch')
    rows = []
    for cells in reader:
        if len(cells) != len(columns):
            raise ValueError('exported CSV row width mismatch')
        rows.append(dict(zip(columns, cells)))
    if len(rows) != report['rows']:
        raise ValueError('exported CSV row count mismatch')
    return {'columns': columns, 'rows': rows, 'values_remain_display_text_not_reimported_business_records': True}
