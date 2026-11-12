import json
from dispatch_log.schema import digest, canonical, text, integer, instant
from dispatch_log.ledger import Conflict


def identity(finding):
    return {key: finding.get(key) for key in ('code', 'visit', 'worker', 'event', 'parent', 'child', 'shift')}


class Cases:
    def __init__(self, materializer):
        self.materializer = materializer
        self.ledger = materializer.ledger
        self.db = self.ledger.db
        self.db.execute('''CREATE TABLE IF NOT EXISTS quality_cases(
            id TEXT PRIMARY KEY,
            report TEXT NOT NULL REFERENCES reports(id),
            finding TEXT NOT NULL,
            finding_digest TEXT NOT NULL,
            opened_by TEXT NOT NULL,
            state TEXT NOT NULL CHECK(state IN ('open','resolved')),
            revision INTEGER NOT NULL,
            evidence TEXT,
            resolution TEXT
        )''')

    def open(self, ident, report, finding_digest, actor):
        text(ident, 'case.id')
        text(actor, 'case.actor')
        result = self.materializer.report(report)['result']
        matches = [row for row in result['quality']['findings'] if digest(row) == finding_digest]
        if not matches:
            raise ValueError('finding does not exist in immutable report')
        finding = matches[0]
        with self.ledger.atomic():
            row = self.db.execute('SELECT * FROM quality_cases WHERE id=?', (ident,)).fetchone()
            if row and (row['report'] != report or row['finding_digest'] != finding_digest or row['opened_by'] != actor):
                raise Conflict('case identifier already binds another finding')
            self.db.execute('INSERT OR IGNORE INTO quality_cases VALUES(?,?,?,?,?,?,1,NULL,NULL)', (ident, report, canonical(finding), finding_digest, actor, 'open'))
        return self.get(ident)

    def get(self, ident):
        row = self.db.execute('SELECT * FROM quality_cases WHERE id=?', (ident,)).fetchone()
        if row is None:
            raise KeyError(ident)
        value = dict(row)
        value['finding'] = json.loads(value['finding'])
        if digest(value['finding']) != value['finding_digest']:
            raise ValueError('case finding integrity failure')
        result = self.materializer.report(value['report'])['result']
        if not any(digest(item) == value['finding_digest'] for item in result['quality']['findings']):
            raise ValueError('case finding/report binding mismatch')
        if value['resolution'] is not None:
            value['resolution'] = json.loads(value['resolution'])
        return value

    def check_evidence(self, case, report):
        before = self.materializer.report(case['report'])['result']
        after = self.materializer.report(report)['result']
        for key in ('start', 'end', 'effective_at', 'offset_minutes'):
            if before[key] != after[key]:
                raise ValueError('resolution evidence must preserve business and worktime windows')
        if instant(after['knowledge_at'], 'knowledge_at') <= instant(before['knowledge_at'], 'knowledge_at'):
            raise ValueError('resolution requires strictly later knowledge')
        target = identity(case['finding'])
        if any(identity(row) == target for row in after['quality']['findings']):
            raise ValueError('finding identity is still present in later report')
        visit_id = case['finding'].get('visit')
        if visit_id is not None:
            visit = next((row for row in after['timeline']['visits'] if row['visit'] == visit_id), None)
            if visit is None or not visit['valid'] or visit['state'] == 'not_requested':
                raise ValueError('resolution cannot hide visit by losing valid evidence')
        worker_id = case['finding'].get('worker')
        if worker_id is not None and worker_id not in self.materializer.dataset(report).workers:
            raise ValueError('resolution cannot remove worker')
        return after['result_digest']

    def resolve(self, ident, revision, evidence_report, actor, reason):
        integer(revision, 'revision', 1)
        text(actor, 'actor')
        text(reason, 'reason')
        with self.ledger.atomic():
            case = self.get(ident)
            if case['state'] != 'open' or case['revision'] != revision:
                raise Conflict('case is closed or revision is stale')
            if actor == case['opened_by']:
                raise Conflict('opener cannot resolve their own case')
            evidence_digest = self.check_evidence(case, evidence_report)
            resolution = {'actor': actor, 'reason': reason, 'evidence_digest': evidence_digest}
            self.db.execute("UPDATE quality_cases SET state='resolved',revision=revision+1,evidence=?,resolution=? WHERE id=?", (evidence_report, canonical(resolution), ident))
        return self.get(ident)

    def audit(self):
        ids = [row[0] for row in self.db.execute('SELECT id FROM quality_cases ORDER BY id')]
        for ident in ids:
            case = self.get(ident)
            if case['state'] == 'open':
                if case['revision'] != 1 or case['evidence'] is not None or case['resolution'] is not None:
                    raise ValueError('open case has inconsistent resolution state')
            else:
                if case['revision'] != 2 or case['resolution'] is None:
                    raise ValueError('resolved case has incomplete decision')
                if case['resolution']['actor'] == case['opened_by']:
                    raise ValueError('resolved case violates separation')
                if self.check_evidence(case, case['evidence']) != case['resolution']['evidence_digest']:
                    raise ValueError('resolved case evidence mismatch')
        return {'valid': True, 'cases': len(ids)}

    def pending(self):
        return [self.get(row[0]) for row in self.db.execute("SELECT id FROM quality_cases WHERE state='open' ORDER BY id")]
