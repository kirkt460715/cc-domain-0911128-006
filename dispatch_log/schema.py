from datetime import datetime, timezone
import copy
import hashlib
import json


class Invalid(ValueError):
    def __init__(self, path, detail):
        self.path = path
        super().__init__(f'{path}: {detail}')


def fields(value, required, optional=(), path='$'):
    if not isinstance(value, dict):
        raise Invalid(path, 'expected object')
    if set(required) - value.keys() or value.keys() - set(required) - set(optional):
        raise Invalid(path, 'missing or unexpected fields')


def text(value, path):
    if not isinstance(value, str) or not value.strip() or len(value) > 120:
        raise Invalid(path, 'expected nonempty bounded text')
    if any(ord(char) < 32 for char in value):
        raise Invalid(path, 'control characters forbidden')
    return value


def integer(value, path, low=0, high=10**12):
    if type(value) is not int or not low <= value <= high:
        raise Invalid(path, 'expected bounded integer')
    return value


def records(value, path, maximum=100000):
    if not isinstance(value, list) or len(value) > maximum:
        raise Invalid(path, 'expected bounded list')
    return value


def instant(value, path):
    if not isinstance(value, str) or len(value) > 40:
        raise Invalid(path, 'expected timezone-aware ISO second')
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError as exc:
        raise Invalid(path, 'invalid timestamp') from exc
    if parsed.tzinfo is None or parsed.microsecond:
        raise Invalid(path, 'timezone required, fractional seconds unsupported')
    timestamp = int(parsed.timestamp())
    if not 0 <= timestamp <= 4102444800:
        raise Invalid(path, 'timestamp outside supported 1970..2100 interval')
    return timestamp


def utc(value):
    integer(value, 'timestamp', 0, 4102444800)
    return datetime.fromtimestamp(value, timezone.utc).isoformat().replace('+00:00', 'Z')


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def unique(rows, path):
    result = {}
    for row in rows:
        if row['id'] in result:
            raise Invalid(path, 'duplicate id')
        result[row['id']] = copy.deepcopy(row)
    return result


def validate_events(rows):
    result = []
    seen = set()
    kinds = {'accepted', 'departed', 'arrived', 'started', 'paused', 'resumed', 'completed', 'cancelled'}
    for row in records(rows, 'events'):
        fields(row, ('id', 'revision', 'visit', 'sequence', 'effective_at', 'observed_at', 'kind', 'worker', 'reason', 'void'))
        text(row['id'], 'event.id')
        text(row['visit'], 'event.visit')
        integer(row['revision'], 'event.revision', 1, 1000000)
        integer(row['sequence'], 'event.sequence', 1, 1000000)
        effective = instant(row['effective_at'], 'event.effective_at')
        observed = instant(row['observed_at'], 'event.observed_at')
        if observed < effective:
            raise Invalid('event.observed_at', 'cannot observe an event before it occurs')
        if row['kind'] not in kinds:
            raise Invalid('event.kind', 'unknown kind')
        if row['worker'] is not None:
            text(row['worker'], 'event.worker')
        if row['reason'] is not None:
            text(row['reason'], 'event.reason')
        if type(row['void']) is not bool:
            raise Invalid('event.void', 'expected boolean')
        if row['kind'] == 'cancelled' and row['reason'] not in ('client', 'provider', 'weather', 'duplicate', 'medical'):
            raise Invalid('event.reason', 'cancel requires known business reason')
        if row['kind'] == 'paused' and row['reason'] not in ('client_break', 'worker_break', 'medical', 'equipment'):
            raise Invalid('event.reason', 'pause requires known business reason')
        key = row['id'], row['revision']
        if key in seen:
            raise Invalid('events', 'duplicate event revision')
        seen.add(key)
        result.append(copy.deepcopy(row))
    return result


class Dataset:
    def __init__(self, document):
        self.document = copy.deepcopy(document)
        fields(self.document, ('version', 'services', 'workers', 'visits', 'shifts', 'events'))
        if type(document['version']) is not int or document['version'] != 1:
            raise Invalid('version', 'only version 1 supported')
        self.services = self._services(self.document['services'])
        self.workers = self._workers(self.document['workers'])
        self.visits = self._visits(self.document['visits'])
        self.shifts = self._shifts(self.document['shifts'])
        self.events = validate_events(self.document['events'])
        for event in self.events:
            if event['visit'] not in self.visits:
                raise Invalid('event.visit', 'unknown visit')
            if event['worker'] is not None and event['worker'] not in self.workers:
                raise Invalid('event.worker', 'unknown worker')
        self.fingerprint = digest(self.document)

    def _services(self, rows):
        for row in records(rows, 'services', 1000):
            fields(row, ('id', 'response_seconds', 'minimum_active_seconds', 'maximum_active_seconds'))
            text(row['id'], 'service.id')
            integer(row['response_seconds'], 'service.response_seconds', 0, 604800)
            integer(row['minimum_active_seconds'], 'service.minimum_active_seconds', 0, 86400)
            integer(row['maximum_active_seconds'], 'service.maximum_active_seconds', 1, 604800)
            if row['minimum_active_seconds'] > row['maximum_active_seconds']:
                raise Invalid('service', 'reversed active duration bounds')
        return unique(rows, 'services')

    def _workers(self, rows):
        for row in records(rows, 'workers', 10000):
            fields(row, ('id', 'region', 'daily_regular_seconds'))
            text(row['id'], 'worker.id')
            text(row['region'], 'worker.region')
            integer(row['daily_regular_seconds'], 'worker.daily_regular_seconds', 0, 86400)
        return unique(rows, 'workers')

    def _visits(self, rows):
        for row in records(rows, 'visits'):
            fields(row, ('id', 'client', 'service', 'region', 'requested_at', 'promised_start', 'parent'))
            for key in ('id', 'client', 'region'):
                text(row[key], 'visit.' + key)
            if row['service'] not in self.services:
                raise Invalid('visit.service', 'unknown service')
            requested = instant(row['requested_at'], 'visit.requested_at')
            promised = instant(row['promised_start'], 'visit.promised_start')
            if promised < requested:
                raise Invalid('visit', 'promise precedes request')
            if row['parent'] is not None:
                text(row['parent'], 'visit.parent')
        output = unique(rows, 'visits')
        for visit in output.values():
            parent = visit['parent']
            if parent is not None:
                if parent not in output or parent == visit['id']:
                    raise Invalid('visit.parent', 'unknown or self parent')
                previous = output[parent]
                if previous['client'] != visit['client'] or previous['service'] != visit['service']:
                    raise Invalid('visit.parent', 'retry must have same client and service')
                if instant(previous['requested_at'], 'parent') >= instant(visit['requested_at'], 'child'):
                    raise Invalid('visit.parent', 'retry request must strictly follow parent request')
        return output

    def _shifts(self, rows):
        for row in records(rows, 'shifts'):
            fields(row, ('id', 'worker', 'start', 'end', 'breaks'))
            text(row['id'], 'shift.id')
            if row['worker'] not in self.workers:
                raise Invalid('shift.worker', 'unknown worker')
            start = instant(row['start'], 'shift.start')
            end = instant(row['end'], 'shift.end')
            if not start < end <= start + 7 * 86400:
                raise Invalid('shift', 'shift must be positive and at most seven days')
            for interval in records(row['breaks'], 'shift.breaks', 1000):
                fields(interval, ('start', 'end'))
                left = instant(interval['start'], 'break.start')
                right = instant(interval['end'], 'break.end')
                if not start <= left < right <= end:
                    raise Invalid('shift.break', 'break must lie inside shift')
        return unique(rows, 'shifts')

    def export(self):
        return copy.deepcopy(self.document)
