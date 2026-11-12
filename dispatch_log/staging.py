import json
from dispatch_log.schema import fields, text, integer, canonical, digest
from dispatch_log.ledger import TABLES, Conflict


class Staging:
    def __init__(self, ledger):
        self.ledger = ledger
        self.db = ledger.db
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS staged_batches(
            id TEXT PRIMARY KEY,
            parts INTEGER NOT NULL,
            expected_digest TEXT NOT NULL,
            promoted TEXT
        );
        CREATE TABLE IF NOT EXISTS staged_parts(
            batch TEXT NOT NULL REFERENCES staged_batches(id),
            part INTEGER NOT NULL,
            body TEXT NOT NULL,
            digest TEXT NOT NULL,
            PRIMARY KEY(batch,part)
        );
        ''')

    def begin(self, ident, parts, expected_digest):
        text(ident, 'staging.id')
        integer(parts, 'parts', 1, 1000)
        if not isinstance(expected_digest, str) or len(expected_digest) != 64 or any(c not in '0123456789abcdef' for c in expected_digest):
            raise ValueError('expected SHA256 hex digest')
        with self.ledger.atomic():
            row = self.db.execute('SELECT * FROM staged_batches WHERE id=?', (ident,)).fetchone()
            if row and (row['parts'] != parts or row['expected_digest'] != expected_digest):
                raise Conflict('staged batch contract is immutable')
            self.db.execute('INSERT OR IGNORE INTO staged_batches VALUES(?,?,?,NULL)', (ident, parts, expected_digest))
        return self.status(ident)

    def status(self, ident):
        row = self.db.execute('SELECT * FROM staged_batches WHERE id=?', (ident,)).fetchone()
        if row is None:
            raise KeyError(ident)
        received = [item[0] for item in self.db.execute('SELECT part FROM staged_parts WHERE batch=? ORDER BY part', (ident,))]
        return {'id': ident, 'parts': row['parts'], 'expected_digest': row['expected_digest'], 'received': received, 'missing': sorted(set(range(row['parts'])) - set(received)), 'promoted_batch': row['promoted']}

    def part(self, ident, index, body):
        fields(body, (*TABLES, 'events'))
        for table in (*TABLES, 'events'):
            if not isinstance(body[table], list) or len(body[table]) > 100000:
                raise ValueError('part tables require bounded lists')
        with self.ledger.atomic():
            state = self.status(ident)
            integer(index, 'part', 0, state['parts'] - 1)
            if state['promoted_batch'] is not None:
                raise Conflict('promoted stage is sealed')
            existing = self.db.execute('SELECT digest FROM staged_parts WHERE batch=? AND part=?', (ident, index)).fetchone()
            if existing and existing[0] != digest(body):
                raise Conflict('part index already binds another body')
            self.db.execute('INSERT OR IGNORE INTO staged_parts VALUES(?,?,?,?)', (ident, index, canonical(body), digest(body)))
        return self.status(ident)

    def combined(self, ident):
        state = self.status(ident)
        if state['missing']:
            raise Conflict('staged batch is incomplete')
        result = {table: [] for table in (*TABLES, 'events')}
        for row in self.db.execute('SELECT * FROM staged_parts WHERE batch=? ORDER BY part', (ident,)):
            body = json.loads(row['body'])
            if digest(body) != row['digest']:
                raise ValueError('staged part integrity failure')
            for table in result:
                result[table].extend(body[table])
        if digest(result) != state['expected_digest']:
            raise ValueError('combined stage digest mismatch')
        return result

    def promote(self, ident, batch_id, expected_generation=None):
        with self.ledger.atomic():
            state = self.status(ident)
            if state['promoted_batch'] is not None:
                if state['promoted_batch'] != batch_id:
                    raise Conflict('stage already promoted to another batch')
                return self.ledger.batch(batch_id)
            body = self.combined(ident)
            receipt = self.ledger.ingest(batch_id, body, expected_generation)
            self.db.execute('UPDATE staged_batches SET promoted=? WHERE id=?', (batch_id, ident))
        return receipt

    def audit(self):
        count = 0
        for row in self.db.execute('SELECT id FROM staged_batches ORDER BY id'):
            state = self.status(row[0])
            if not state['missing']:
                body = self.combined(row[0])
                if state['promoted_batch'] is not None:
                    if self.ledger.batch(state['promoted_batch'])['request_digest'] != digest(body):
                        raise ValueError('staged promotion/batch binding mismatch')
            count += 1
        return {'valid': True, 'staged_batches': count}
