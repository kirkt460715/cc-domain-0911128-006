import json
from collections import defaultdict
from dispatch_log.schema import digest, canonical, integer, text
from dispatch_log.ledger import Conflict
from quality.sla import summarize


def safe_sla(result, dimension='region', minimum_clients=3):
    integer(minimum_clients, 'minimum_clients', 1, 1000000)
    if dimension not in ('region', 'service'):
        raise ValueError('public SLA grouping must be region or service')
    report = summarize(result['sla_visits'], [dimension])
    clients = defaultdict(set)
    for row in result['sla_visits']:
        if not row['future_request']:
            clients[row[dimension]].add(row['client'])
    hidden = [index for index, row in enumerate(report['rows']) if len(clients[row[dimension]]) < minimum_clients]
    if len(hidden) == 1 and len(report['rows']) > 1:
        candidates = [index for index in range(len(report['rows'])) if index not in hidden]
        hidden.append(min(candidates, key=lambda index: (len(clients[report['rows'][index][dimension]]), index)))
    output = []
    for index, row in enumerate(report['rows']):
        if index in hidden:
            output.append({dimension: row[dimension], 'suppressed': True})
        else:
            output.append({**row, 'distinct_clients': len(clients[row[dimension]]), 'suppressed': False})
    overall = result['sla_overall'] if not hidden else None
    body = {
        'result_digest': result['result_digest'],
        'dimension': dimension,
        'minimum_clients': minimum_clients,
        'rows': output,
        'overall': overall,
        'contains_visit_or_worker_ids': False,
        'limitations': 'small_partition_and_complementary_suppression_not_arbitrary_composition_privacy',
        'sla_scope': result['scope'],
    }
    return {**body, 'publication_digest': digest(body)}


def verify(packet):
    if digest({key: value for key, value in packet.items() if key != 'publication_digest'}) != packet.get('publication_digest'):
        raise ValueError('publication checksum mismatch')
    if packet['contains_visit_or_worker_ids']:
        raise ValueError('public packet contains restricted identifiers')
    hidden = False
    for row in packet['rows']:
        if row['suppressed']:
            hidden = True
            if set(row) != {packet['dimension'], 'suppressed'}:
                raise ValueError('suppressed row contains measures')
    if hidden and packet['overall'] is not None:
        raise ValueError('public total reveals suppressed cells')
    return True


class Publisher:
    def __init__(self, materializer):
        self.materializer = materializer
        self.ledger = materializer.ledger
        self.db = self.ledger.db
        self.db.execute('''CREATE TABLE IF NOT EXISTS publications(
            label TEXT PRIMARY KEY,
            report TEXT NOT NULL REFERENCES reports(id),
            body TEXT NOT NULL,
            digest TEXT NOT NULL
        )''')

    def publish(self, label, report_id, dimension='region', minimum_clients=3):
        text(label, 'label')
        result = self.materializer.report(report_id)['result']
        packet = safe_sla(result, dimension, minimum_clients)
        verify(packet)
        with self.ledger.atomic():
            row = self.db.execute('SELECT * FROM publications WHERE label=?', (label,)).fetchone()
            if row and (row['report'] != report_id or row['digest'] != packet['publication_digest']):
                raise Conflict('publication label is immutable')
            self.db.execute('INSERT OR IGNORE INTO publications VALUES(?,?,?,?)', (label, report_id, canonical(packet), packet['publication_digest']))
        return self.get(label)

    def get(self, label):
        row = self.db.execute('SELECT * FROM publications WHERE label=?', (label,)).fetchone()
        if row is None:
            raise KeyError(label)
        packet = json.loads(row['body'])
        verify(packet)
        if packet['publication_digest'] != row['digest'] or packet['result_digest'] != self.materializer.report(row['report'])['result']['result_digest']:
            raise ValueError('publication/report binding mismatch')
        return {'label': label, 'report': row['report'], 'packet': packet}

    def audit(self):
        labels = [row[0] for row in self.db.execute('SELECT label FROM publications ORDER BY label')]
        for label in labels:
            self.get(label)
        return {'valid': True, 'publications': len(labels)}
