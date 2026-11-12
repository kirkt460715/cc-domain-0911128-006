from collections import defaultdict
import json
from dispatch_log.schema import instant, digest, canonical
from dispatch_log.selection import select
from dispatch_log.replay import replay_visit
from operations.pipeline import analyze, audit


class Materializer:
    def __init__(self, ledger):
        self.ledger = ledger
        self.db = ledger.db
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS visit_cache(
            key TEXT PRIMARY KEY,
            request TEXT NOT NULL,
            body TEXT NOT NULL,
            body_digest TEXT NOT NULL,
            hits INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS reports(
            id TEXT PRIMARY KEY,
            request TEXT NOT NULL,
            request_digest TEXT NOT NULL,
            generation INTEGER NOT NULL,
            source TEXT NOT NULL,
            body TEXT NOT NULL,
            body_digest TEXT NOT NULL,
            cache_stats TEXT NOT NULL
        );
        ''')

    def cached_replay(self, dataset, effective_at, knowledge_at):
        business = instant(effective_at, 'effective_at')
        if instant(knowledge_at, 'knowledge_at') < business:
            raise ValueError('knowledge cutoff precedes business cutoff')
        selection = select(dataset.events, knowledge_at)
        events = defaultdict(list)
        issues = defaultdict(list)
        for row in selection['events']:
            events[row['visit']].append(row)
        for issue in selection['issues']:
            targets = {row['visit'] for row in dataset.events if row['id'] == issue['event']}
            for target in targets:
                issues[target].append(issue)
        timelines = []
        reused = 0
        calculated = 0
        for ident, visit in sorted(dataset.visits.items()):
            relevant = sorted([row for row in events[ident] if instant(row['effective_at'], 'effective_at') <= business], key=lambda row: (row['sequence'], row['id']))
            request = {'visit': visit, 'events': relevant, 'issues': issues[ident], 'effective_at': effective_at, 'algorithm': 1}
            key = digest(request)
            existing = self.db.execute('SELECT * FROM visit_cache WHERE key=?', (key,)).fetchone()
            if existing:
                body = json.loads(existing['body'])
                if digest(body) != existing['body_digest'] or json.loads(existing['request']) != request:
                    raise ValueError('visit contribution cache integrity failure')
                self.db.execute('UPDATE visit_cache SET hits=hits+1 WHERE key=?', (key,))
                reused += 1
            else:
                body = replay_visit(dataset, visit, relevant, effective_at, issues[ident])
                body = json.loads(canonical(body))
                with self.ledger.atomic():
                    self.db.execute('INSERT OR IGNORE INTO visit_cache(key,request,body,body_digest) VALUES(?,?,?,?)', (key, canonical(request), canonical(body), digest(body)))
                calculated += 1
            timelines.append(body)
        body = {'input_digest': dataset.fingerprint, 'effective_at': effective_at, 'knowledge_at': knowledge_at, 'selection': selection, 'visits': timelines}
        return {**body, 'replay_digest': digest(body)}, {'reused_visits': reused, 'recomputed_visits': calculated, 'worker_overlap_and_daily_aggregates_recomputed': True}

    def produce(self, ident, start, end, effective_at, knowledge_at, offset_minutes=0, expected_generation=None):
        from dispatch_log.schema import text, integer
        text(ident, 'report.id')
        generation = self.ledger.generation()
        if expected_generation is not None:
            integer(expected_generation, 'expected_generation')
            if expected_generation != generation:
                from dispatch_log.ledger import Conflict
                raise Conflict('report generation is stale')
        dataset = self.ledger.document()
        request = {'start': start, 'end': end, 'effective_at': effective_at, 'knowledge_at': knowledge_at, 'offset_minutes': offset_minutes, 'input_digest': dataset.fingerprint}
        existing = self.db.execute('SELECT request_digest FROM reports WHERE id=?', (ident,)).fetchone()
        if existing:
            if existing[0] != digest(request):
                from dispatch_log.ledger import Conflict
                raise Conflict('report id binds another source or cutoff')
            return self.report(ident)
        timeline, stats = self.cached_replay(dataset, effective_at, knowledge_at)
        result = analyze(dataset, start, end, effective_at, knowledge_at, offset_minutes, timeline)
        audit(dataset, result)
        with self.ledger.atomic():
            if self.ledger.generation() != generation:
                from dispatch_log.ledger import Conflict
                raise Conflict('ledger changed during report; result not committed')
            row = self.db.execute('SELECT body_digest FROM reports WHERE id=?', (ident,)).fetchone()
            if row and row[0] != result['result_digest']:
                from dispatch_log.ledger import Conflict
                raise Conflict('another report request used this id')
            self.db.execute('INSERT OR IGNORE INTO reports VALUES(?,?,?,?,?,?,?,?)', (ident, canonical(request), digest(request), generation, canonical(dataset.document), canonical(result), result['result_digest'], canonical(stats)))
        return self.report(ident)

    def report(self, ident):
        row = self.db.execute('SELECT * FROM reports WHERE id=?', (ident,)).fetchone()
        if row is None:
            raise KeyError(ident)
        request = json.loads(row['request'])
        result = json.loads(row['body'])
        if digest(request) != row['request_digest'] or result['result_digest'] != row['body_digest']:
            raise ValueError('report storage checksum mismatch')
        from dispatch_log.schema import Dataset
        dataset = Dataset(json.loads(row['source']))
        if dataset.fingerprint != request['input_digest']:
            raise ValueError('report stored source mismatch')
        for key in ('start', 'end', 'effective_at', 'knowledge_at', 'offset_minutes'):
            if request[key] != result[key]:
                raise ValueError('report request/result context mismatch')
        audit(dataset, result)
        return {'id': ident, 'generation': row['generation'], 'request': request, 'result': result, 'cache': json.loads(row['cache_stats'])}

    def dataset(self, ident):
        from dispatch_log.schema import Dataset
        row = self.db.execute('SELECT source FROM reports WHERE id=?', (ident,)).fetchone()
        if row is None:
            raise KeyError(ident)
        return Dataset(json.loads(row[0]))

    def audit(self, replay_all=True):
        ids = [row[0] for row in self.db.execute('SELECT id FROM reports ORDER BY id')]
        for ident in ids:
            result = self.report(ident)
            audit(self.dataset(ident), result['result'], replay_all)
        cache = self.cache_audit()
        return {'valid': True, 'reports': len(ids), 'cached_visit_contributions': cache['entries']}

    def cache_audit(self):
        from dispatch_log.schema import fields, integer
        entries = 0
        hits = 0
        per_visit = {}
        for row in self.db.execute('SELECT * FROM visit_cache ORDER BY key'):
            request = json.loads(row['request'])
            body = json.loads(row['body'])
            fields(request, ('visit', 'events', 'issues', 'effective_at', 'algorithm'))
            if request['algorithm'] != 1:
                raise ValueError('unsupported cached replay algorithm')
            if digest(request) != row['key']:
                raise ValueError('visit cache request checksum failure')
            if digest(body) != row['body_digest']:
                raise ValueError('visit cache body checksum failure')
            integer(row['hits'], 'cache.hits')
            visit = request['visit']
            if not isinstance(visit, dict) or 'id' not in visit:
                raise ValueError('visit cache lacks visit identity')
            if body['visit'] != visit['id']:
                raise ValueError('visit cache result identity mismatch')
            if any(event['visit'] != visit['id'] for event in request['events']):
                raise ValueError('visit cache contains foreign event')
            expected = replay_visit(None, visit, request['events'], request['effective_at'], request['issues'])
            if json.loads(canonical(expected)) != body:
                raise ValueError('visit cache semantic replay mismatch')
            group = per_visit.setdefault(visit['id'], {'entries': 0, 'hits': 0})
            group['entries'] += 1
            group['hits'] += row['hits']
            entries += 1
            hits += row['hits']
        return {
            'valid': True,
            'entries': entries,
            'hits': hits,
            'visits': [{'visit': key, **value} for key, value in sorted(per_visit.items())],
            'key_scope': 'immutable_visit_selected_event_revisions_selection_issues_business_cutoff_algorithm',
            'full_worker_overlap_and_daily_totals_are_never_cached': True,
        }
