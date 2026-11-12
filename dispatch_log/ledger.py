from contextlib import contextmanager
import json
import sqlite3
from dispatch_log.schema import Dataset, fields, text, integer, records, canonical, digest


class Conflict(ValueError):
    pass


TABLES = ('services', 'workers', 'visits', 'shifts')


class Ledger:
    def __init__(self, path):
        self.transaction_serial = 0
        self.db = sqlite3.connect(str(path), isolation_level=None, timeout=5)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA foreign_keys=ON')
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS generation(
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            revision INTEGER NOT NULL
        );
        INSERT OR IGNORE INTO generation VALUES(1,0);
        CREATE TABLE IF NOT EXISTS registry(
            collection TEXT NOT NULL,
            id TEXT NOT NULL,
            body TEXT NOT NULL,
            digest TEXT NOT NULL,
            PRIMARY KEY(collection,id)
        );
        CREATE TABLE IF NOT EXISTS events(
            id TEXT NOT NULL,
            revision INTEGER NOT NULL,
            visit TEXT NOT NULL,
            body TEXT NOT NULL,
            digest TEXT NOT NULL,
            PRIMARY KEY(id,revision)
        );
        CREATE TABLE IF NOT EXISTS batches(
            id TEXT PRIMARY KEY,
            sequence INTEGER NOT NULL UNIQUE,
            request TEXT NOT NULL,
            request_digest TEXT NOT NULL,
            generation INTEGER NOT NULL,
            previous TEXT,
            checksum TEXT NOT NULL,
            changes TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS events_visit ON events(visit,id,revision);
        ''')

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        self.db.close()

    @contextmanager
    def atomic(self):
        nested = self.db.in_transaction
        self.transaction_serial += 1
        savepoint = 'ledger_' + str(self.transaction_serial)
        self.db.execute('SAVEPOINT ' + savepoint if nested else 'BEGIN IMMEDIATE')
        try:
            yield
        except BaseException:
            if nested:
                self.db.execute('ROLLBACK TO ' + savepoint)
                self.db.execute('RELEASE ' + savepoint)
            else:
                self.db.execute('ROLLBACK')
            raise
        else:
            self.db.execute('RELEASE ' + savepoint if nested else 'COMMIT')

    def generation(self):
        return self.db.execute('SELECT revision FROM generation WHERE singleton=1').fetchone()[0]

    def document(self):
        document = {'version': 1}
        for table in TABLES:
            values = []
            for row in self.db.execute('SELECT * FROM registry WHERE collection=? ORDER BY id', (table,)):
                body = json.loads(row['body'])
                if digest(body) != row['digest'] or body['id'] != row['id']:
                    raise ValueError('registry integrity failure')
                values.append(body)
            document[table] = values
        values = []
        for row in self.db.execute('SELECT * FROM events ORDER BY id,revision'):
            body = json.loads(row['body'])
            if digest(body) != row['digest'] or body['id'] != row['id'] or body['revision'] != row['revision'] or body['visit'] != row['visit']:
                raise ValueError('event integrity failure')
            values.append(body)
        document['events'] = values
        return Dataset(document)

    def ingest(self, ident, batch, expected_generation=None):
        text(ident, 'batch.id')
        fields(batch, (*TABLES, 'events'))
        if expected_generation is not None:
            integer(expected_generation, 'expected_generation')
        for table in (*TABLES, 'events'):
            records(batch[table], 'batch.' + table)
        signature = digest(batch)
        with self.atomic():
            existing = self.db.execute('SELECT request_digest FROM batches WHERE id=?', (ident,)).fetchone()
            if existing:
                if existing[0] != signature:
                    raise Conflict('batch id already binds another payload')
                return self.batch(ident)
            revision = self.generation()
            if expected_generation is not None and revision != expected_generation:
                raise Conflict('ledger generation changed')
            current = self.document().export()
            changes = []
            for table in TABLES:
                known = {row['id']: row for row in current[table]}
                seen = set()
                for row in batch[table]:
                    if not isinstance(row, dict) or 'id' not in row:
                        raise ValueError('registry row requires id')
                    key = row['id']
                    text(key, 'registry.id')
                    if key in seen:
                        raise ValueError('duplicate registry record in batch')
                    seen.add(key)
                    if key in known:
                        if known[key] != row:
                            raise Conflict('registry records are immutable')
                    else:
                        current[table].append(row)
                        changes.append({'table': table, 'id': key})
            known_events = {(row['id'], row['revision']): row for row in current['events']}
            visits_by_event = {row['id']: row['visit'] for row in current['events']}
            seen = set()
            for row in batch['events']:
                if not isinstance(row, dict) or not {'id', 'revision', 'visit'} <= row.keys():
                    raise ValueError('event key missing')
                key = row['id'], row['revision']
                if key in seen:
                    raise ValueError('duplicate event revision within batch')
                seen.add(key)
                if row['id'] in visits_by_event and visits_by_event[row['id']] != row['visit']:
                    raise Conflict('event identity cannot move to another visit')
                visits_by_event[row['id']] = row['visit']
                if key in known_events:
                    if known_events[key] != row:
                        raise Conflict('existing event revision is immutable')
                else:
                    current['events'].append(row)
                    changes.append({'table': 'events', 'id': row['id'], 'revision': row['revision'], 'visit': row['visit']})
            Dataset(current)
            for change in changes:
                table = change['table']
                if table == 'events':
                    row = next(row for row in batch['events'] if row['id'] == change['id'] and row['revision'] == change['revision'])
                    self.db.execute('INSERT INTO events VALUES(?,?,?,?,?)', (row['id'], row['revision'], row['visit'], canonical(row), digest(row)))
                else:
                    row = next(row for row in batch[table] if row['id'] == change['id'])
                    self.db.execute('INSERT INTO registry VALUES(?,?,?,?)', (table, row['id'], canonical(row), digest(row)))
            generation = revision + bool(changes)
            self.db.execute('UPDATE generation SET revision=? WHERE singleton=1', (generation,))
            previous = self.db.execute('SELECT sequence,checksum FROM batches ORDER BY sequence DESC LIMIT 1').fetchone()
            sequence = previous['sequence'] + 1 if previous else 1
            previous_hash = previous['checksum'] if previous else None
            receipt = {'id': ident, 'sequence': sequence, 'request_digest': signature, 'generation': generation, 'previous': previous_hash, 'changes': changes}
            self.db.execute('INSERT INTO batches VALUES(?,?,?,?,?,?,?,?)', (ident, sequence, canonical(batch), signature, generation, previous_hash, digest(receipt), canonical(changes)))
        return self.batch(ident)

    def batch(self, ident):
        row = self.db.execute('SELECT * FROM batches WHERE id=?', (ident,)).fetchone()
        if row is None:
            raise KeyError(ident)
        receipt = {'id': ident, 'sequence': row['sequence'], 'request_digest': row['request_digest'], 'generation': row['generation'], 'previous': row['previous'], 'changes': json.loads(row['changes'])}
        if digest(receipt) != row['checksum'] or digest(json.loads(row['request'])) != row['request_digest']:
            raise ValueError('batch integrity failure')
        return {**receipt, 'checksum': row['checksum']}

    def import_document(self, ident, document, expected_generation=None):
        dataset = Dataset(document)
        return self.ingest(ident, {key: dataset.document[key] for key in (*TABLES, 'events')}, expected_generation)

    def export_batches(self):
        values = []
        for row in self.db.execute('SELECT id,request FROM batches ORDER BY sequence'):
            values.append({'receipt': self.batch(row['id']), 'request': json.loads(row['request'])})
        return values

    def audit(self):
        if self.db.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
            raise ValueError('ledger SQLite integrity failure')
        dataset = self.document()
        previous = None
        generation = 0
        for index, batch in enumerate(self.export_batches(), 1):
            row = batch['receipt']
            if row['sequence'] != index or row['previous'] != previous:
                raise ValueError('batch hash chain sequence failure')
            if row['generation'] != generation + bool(row['changes']):
                raise ValueError('batch generation does not track changes')
            generation = row['generation']
            previous = row['checksum']
        if generation != self.generation():
            raise ValueError('ledger head generation mismatch')
        with Ledger(':memory:') as rebuilt:
            for batch in self.export_batches():
                rebuilt.ingest(batch['receipt']['id'], batch['request'])
            if rebuilt.document().fingerprint != dataset.fingerprint:
                raise ValueError('ledger rows differ from replayed batches')
        return {'valid': True, 'generation': generation, 'input_digest': dataset.fingerprint, 'batches': len(self.export_batches()), 'local_chain_not_external_signature': True}
