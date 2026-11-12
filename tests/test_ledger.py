import copy
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch
from dispatch_log.schema import Dataset, digest, canonical
from dispatch_log.ledger import Ledger, Conflict, TABLES
from dispatch_log.staging import Staging
from operations.materializer import Materializer
from operations import recovery


class LedgerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'visits.db'
        self.data = json.loads(Path('fixtures/visits.json').read_text())
        self.ledger = Ledger(self.path)
        self.addCleanup(self.ledger.close)
        self.initial = self.ledger.import_document('seed', self.data, 0)
        self.start = '2026-11-10T00:00:00Z'
        self.end = '2026-11-12T00:00:00Z'
        self.known = '2026-11-12T12:00:00Z'

    def empty(self):
        return {key: [] for key in (*TABLES, 'events')}

    def revision(self, event='v1-7', **values):
        row = next(copy.deepcopy(row) for row in self.data['events'] if row['id'] == event)
        row.update(revision=2, observed_at='2026-11-12T13:00:00Z')
        row.update(values)
        body = self.empty()
        body['events'] = [row]
        return body

    def produce(self, materializer, ident='report', known=None):
        return materializer.produce(ident, self.start, self.end, self.end, known or self.known)

    def test_seed_receipt_and_audit(self):
        self.assertEqual(self.initial['generation'], 1)
        self.assertEqual(self.initial['sequence'], 1)
        self.assertIsNone(self.initial['previous'])
        self.assertEqual(self.ledger.generation(), 1)
        self.assertEqual(len(self.ledger.document().events), 24)
        self.assertEqual(self.ledger.document().visits['retry']['parent'], 'cancel')
        checked = self.ledger.audit()
        self.assertTrue(checked['valid'])
        self.assertEqual(checked['batches'], 1)
        self.assertEqual(self.ledger.batch('seed'), self.initial)

    def test_duplicate_batch_is_idempotent_even_with_old_generation(self):
        replay = self.ledger.import_document('seed', self.data, 0)
        self.assertEqual(replay, self.initial)
        self.assertEqual(self.ledger.generation(), 1)
        changed = copy.deepcopy(self.data)
        changed['workers'][0]['region'] = 'new-region'
        with self.assertRaises(Conflict):
            self.ledger.import_document('seed', changed)
        self.assertEqual(self.ledger.batch('seed'), self.initial)
        self.assertEqual(self.ledger.audit()['batches'], 1)

    def test_new_batch_with_existing_records_has_no_generation_bump(self):
        receipt = self.ledger.import_document('same-source', self.data, 1)
        self.assertEqual(receipt['changes'], [])
        self.assertEqual(receipt['generation'], 1)
        self.assertEqual(receipt['sequence'], 2)
        self.assertEqual(receipt['previous'], self.initial['checksum'])
        self.assertEqual(self.ledger.generation(), 1)
        self.assertTrue(self.ledger.audit()['valid'])
        self.assertEqual(len(self.ledger.export_batches()), 2)

    def test_failed_registry_change_rolls_back_entire_batch(self):
        before = self.ledger.document().fingerprint
        body = self.empty()
        body['workers'] = [{**self.data['workers'][0], 'id': 'new-worker'}, {**self.data['workers'][1], 'region': 'changed'}]
        with self.assertRaises(Conflict):
            self.ledger.ingest('conflicting', body, 1)
        self.assertEqual(self.ledger.document().fingerprint, before)
        self.assertNotIn('new-worker', self.ledger.document().workers)
        with self.assertRaises(KeyError):
            self.ledger.batch('conflicting')
        self.assertEqual(self.ledger.generation(), 1)
        self.assertFalse(self.ledger.db.in_transaction)

    def test_reference_validation_rolls_back(self):
        body = self.empty()
        body['events'] = [{**self.data['events'][0], 'id': 'new-event', 'visit': 'unknown'}]
        before = self.ledger.document().fingerprint
        with self.assertRaises(ValueError):
            self.ledger.ingest('bad-reference', body)
        self.assertEqual(self.ledger.document().fingerprint, before)
        self.assertEqual(self.ledger.generation(), 1)
        self.assertEqual(self.ledger.audit()['batches'], 1)
        self.assertFalse(self.ledger.db.in_transaction)

    def test_event_revision_cannot_move_visit(self):
        body = self.revision(event='v1-7', visit='v2')
        with self.assertRaises(Conflict):
            self.ledger.ingest('move', body)
        self.assertEqual(self.ledger.generation(), 1)
        self.assertEqual(len(self.ledger.document().events), 24)
        self.assertEqual(self.ledger.audit()['batches'], 1)
        body = self.revision()
        self.assertEqual(self.ledger.ingest('correct', body)['generation'], 2)
        self.assertEqual(next(row for row in self.ledger.document().events if row['id'] == 'v1-7' and row['revision'] == 2)['revision'], 2)

    def test_revision_identity_is_immutable(self):
        body = self.revision(effective_at='2026-11-10T10:20:00Z')
        receipt = self.ledger.ingest('correction', body, 1)
        self.assertEqual(receipt['generation'], 2)
        again = self.ledger.ingest('correction-copy', body, 2)
        self.assertEqual(again['changes'], [])
        body['events'][0]['effective_at'] = '2026-11-10T10:30:00Z'
        with self.assertRaises(Conflict):
            self.ledger.ingest('alter', body, 2)
        self.assertEqual(self.ledger.generation(), 2)
        self.assertTrue(self.ledger.audit()['valid'])

    def test_stale_generation_and_duplicate_keys(self):
        body = self.revision()
        with self.assertRaises(Conflict):
            self.ledger.ingest('stale', body, 0)
        body['events'].append(copy.deepcopy(body['events'][0]))
        with self.assertRaises(ValueError):
            self.ledger.ingest('duplicate-event', body, 1)
        duplicate = self.empty()
        duplicate['workers'] = [self.data['workers'][0], self.data['workers'][0]]
        with self.assertRaises(ValueError):
            self.ledger.ingest('duplicate-worker', duplicate)
        self.assertEqual(self.ledger.generation(), 1)
        self.assertEqual(self.ledger.audit()['batches'], 1)

    def test_nested_transaction_rolls_back_outer_and_can_retry(self):
        body = self.revision()
        with self.assertRaisesRegex(RuntimeError, 'power loss'):
            with self.ledger.atomic():
                self.ledger.ingest('nested', body, 1)
                self.assertEqual(self.ledger.generation(), 2)
                raise RuntimeError('power loss')
        self.assertEqual(self.ledger.generation(), 1)
        with self.assertRaises(KeyError):
            self.ledger.batch('nested')
        self.assertEqual(self.ledger.ingest('nested', body, 1)['generation'], 2)
        self.assertTrue(self.ledger.audit()['valid'])

    def test_inner_failure_does_not_abort_outer_work(self):
        with self.ledger.atomic():
            self.ledger.ingest('empty-before', self.empty(), 1)
            with self.assertRaises(Conflict):
                self.ledger.ingest('stale-inner', self.revision(), 0)
            self.ledger.ingest('empty-after', self.empty(), 1)
        self.assertEqual(self.ledger.audit()['batches'], 3)
        self.assertEqual(self.ledger.generation(), 1)
        self.assertFalse(self.ledger.db.in_transaction)
        with self.assertRaises(KeyError):
            self.ledger.batch('stale-inner')

    def test_stage_is_sealed_and_idempotent(self):
        stage = Staging(self.ledger)
        body = self.revision()
        state = stage.begin('stage', 2, digest(body))
        self.assertEqual(state['missing'], [0, 1])
        stage.part('stage', 1, body)
        with self.assertRaises(Conflict):
            stage.promote('stage', 'promoted', 1)
        stage.part('stage', 0, self.empty())
        receipt = stage.promote('stage', 'promoted', 1)
        self.assertEqual(receipt['generation'], 2)
        self.assertEqual(stage.promote('stage', 'promoted', 0), receipt)
        self.assertEqual(stage.status('stage')['promoted_batch'], 'promoted')
        with self.assertRaises(Conflict):
            stage.part('stage', 0, self.empty())
        with self.assertRaises(Conflict):
            stage.promote('stage', 'different')
        self.assertTrue(stage.audit()['valid'])

    def test_stage_contract_and_part_binding(self):
        stage = Staging(self.ledger)
        body = self.revision()
        first = stage.begin('s', 1, digest(body))
        self.assertEqual(stage.begin('s', 1, digest(body)), first)
        with self.assertRaises(Conflict):
            stage.begin('s', 2, digest(body))
        stage.part('s', 0, body)
        self.assertEqual(stage.part('s', 0, body)['received'], [0])
        with self.assertRaises(Conflict):
            stage.part('s', 0, self.empty())
        with self.assertRaises(ValueError):
            stage.part('s', 1, body)
        self.assertIsNone(stage.status('s')['promoted_batch'])
        self.assertEqual(self.ledger.generation(), 1)

    def test_promotion_rollback_includes_ingest_and_seal(self):
        stage = Staging(self.ledger)
        body = self.revision()
        stage.begin('s', 1, digest(body))
        stage.part('s', 0, body)
        self.ledger.db.execute("CREATE TRIGGER reject_seal BEFORE UPDATE ON staged_batches BEGIN SELECT RAISE(ABORT,'seal failure'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            stage.promote('s', 'promoted', 1)
        self.assertEqual(self.ledger.generation(), 1)
        self.assertIsNone(stage.status('s')['promoted_batch'])
        with self.assertRaises(KeyError):
            self.ledger.batch('promoted')
        self.ledger.db.execute('DROP TRIGGER reject_seal')
        self.assertEqual(stage.promote('s', 'promoted', 1)['generation'], 2)
        self.assertTrue(stage.audit()['valid'])

    def test_stage_digest_mismatch_preserves_received_parts(self):
        stage = Staging(self.ledger)
        stage.begin('s', 1, digest(self.empty()))
        stage.part('s', 0, self.revision())
        with self.assertRaisesRegex(ValueError, 'digest mismatch'):
            stage.promote('s', 'bad')
        state = stage.status('s')
        self.assertEqual(state['received'], [0])
        self.assertIsNone(state['promoted_batch'])
        self.assertEqual(self.ledger.generation(), 1)
        with self.assertRaises(KeyError):
            self.ledger.batch('bad')

    def test_concurrent_promotion_commits_once(self):
        stage = Staging(self.ledger)
        body = self.revision()
        stage.begin('s', 1, digest(body))
        stage.part('s', 0, body)
        barrier = threading.Barrier(2)
        results = []
        errors = []
        def worker():
            try:
                with Ledger(self.path) as ledger:
                    service = Staging(ledger)
                    barrier.wait(timeout=5)
                    results.append(service.promote('s', 'promoted', 1))
            except BaseException as exc:
                errors.append(exc)
        threads = [threading.Thread(target=worker) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
            self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0], results[1])
        self.assertEqual(self.ledger.generation(), 2)
        self.assertEqual(self.ledger.audit()['batches'], 2)
        self.assertTrue(stage.audit()['valid'])

    def test_concurrent_different_batches_reject_one_stale_writer(self):
        barrier = threading.Barrier(2)
        results = []
        def worker(ident, event):
            with Ledger(self.path) as ledger:
                body = self.revision(event=event)
                barrier.wait(timeout=5)
                try:
                    ledger.ingest(ident, body, 1)
                    results.append('committed')
                except Conflict:
                    results.append('stale')
        threads = [threading.Thread(target=worker, args=('one', 'v1-7')), threading.Thread(target=worker, args=('two', 'v2-5'))]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
            self.assertFalse(thread.is_alive())
        self.assertEqual(sorted(results), ['committed', 'stale'])
        self.assertEqual(self.ledger.generation(), 2)
        self.assertEqual(self.ledger.audit()['batches'], 2)
        self.assertEqual(len(self.ledger.document().events), 25)

    def test_cache_reuses_only_unchanged_visit_contributions(self):
        materializer = Materializer(self.ledger)
        first = self.produce(materializer, 'one')
        self.assertEqual(first['cache']['recomputed_visits'], 6)
        second = self.produce(materializer, 'two')
        self.assertEqual(second['cache']['reused_visits'], 6)
        self.assertEqual(second['result'], first['result'])
        self.ledger.ingest('correction', self.revision(effective_at='2026-11-10T10:30:00Z'), 1)
        later = self.produce(materializer, 'later', '2026-11-12T14:00:00Z')
        self.assertEqual(later['cache']['recomputed_visits'], 1)
        self.assertEqual(later['cache']['reused_visits'], 5)
        old = materializer.report('one')
        self.assertEqual(old, first)
        self.assertEqual(old['generation'], 1)
        self.assertEqual(later['generation'], 2)
        self.assertNotEqual(later['result']['result_digest'], old['result']['result_digest'])
        self.assertTrue(materializer.audit()['valid'])

    def test_unknown_revision_does_not_invalidate_old_knowledge_contribution(self):
        materializer = Materializer(self.ledger)
        first = self.produce(materializer, 'one')
        self.ledger.ingest('late', self.revision(effective_at='2026-11-10T10:30:00Z'))
        old_knowledge = self.produce(materializer, 'old-knowledge')
        self.assertEqual(old_knowledge['cache']['reused_visits'], 6)
        self.assertEqual(old_knowledge['cache']['recomputed_visits'], 0)
        self.assertEqual(old_knowledge['result']['sla_overall'], first['result']['sla_overall'])
        self.assertNotEqual(old_knowledge['result']['input_digest'], first['result']['input_digest'])
        self.assertEqual(materializer.report('one'), first)
        self.assertTrue(materializer.audit()['valid'])

    def test_report_id_and_generation_bindings(self):
        materializer = Materializer(self.ledger)
        first = self.produce(materializer)
        self.assertEqual(self.produce(materializer), first)
        with self.assertRaises(Conflict):
            self.produce(materializer, known='2026-11-12T14:00:00Z')
        with self.assertRaises(Conflict):
            materializer.produce('stale', self.start, self.end, self.end, self.known, expected_generation=0)
        self.ledger.ingest('correction', self.revision())
        with self.assertRaises(Conflict):
            self.produce(materializer)
        self.assertEqual(materializer.report('report'), first)
        self.assertEqual(materializer.dataset('report').fingerprint, first['request']['input_digest'])

    def test_report_detects_source_change_during_computation(self):
        materializer = Materializer(self.ledger)
        import operations.materializer as module
        original = module.analyze
        def concurrent_change(*args, **kwargs):
            with Ledger(self.path) as writer:
                writer.ingest('concurrent', self.revision(), 1)
            return original(*args, **kwargs)
        with patch.object(module, 'analyze', side_effect=concurrent_change):
            with self.assertRaisesRegex(Conflict, 'ledger changed'):
                self.produce(materializer)
        with self.assertRaises(KeyError):
            materializer.report('report')
        self.assertEqual(self.ledger.generation(), 2)
        self.assertEqual(self.produce(materializer)['generation'], 2)
        self.assertTrue(materializer.audit()['valid'])

    def test_cache_tamper_is_rejected(self):
        materializer = Materializer(self.ledger)
        self.produce(materializer)
        self.ledger.db.execute("UPDATE visit_cache SET body='{}' WHERE key=(SELECT key FROM visit_cache LIMIT 1)")
        with self.assertRaisesRegex(ValueError, 'cache integrity'):
            self.produce(materializer, 'new')
        with self.assertRaises(ValueError):
            materializer.audit()
        self.assertEqual(self.ledger.generation(), 1)
        self.assertTrue(self.ledger.audit()['valid'])

    def test_ledger_tamper_detects_digest_and_semantic_replay(self):
        row = self.ledger.db.execute("SELECT * FROM registry WHERE collection='workers' AND id='w1'").fetchone()
        body = json.loads(row['body'])
        body['region'] = 'tampered'
        self.ledger.db.execute("UPDATE registry SET body=? WHERE collection='workers' AND id='w1'", (canonical(body),))
        with self.assertRaisesRegex(ValueError, 'registry integrity'):
            self.ledger.document()
        self.ledger.db.execute("UPDATE registry SET digest=? WHERE collection='workers' AND id='w1'", (digest(body),))
        with self.assertRaisesRegex(ValueError, 'replayed batches'):
            self.ledger.audit()
        self.assertEqual(self.ledger.generation(), 1)

    def test_backup_restore_and_ledger_only_rebuild(self):
        materializer = Materializer(self.ledger)
        first = self.produce(materializer)
        destination = Path(self.tmp.name) / 'backup.db'
        saved = recovery.backup(self.ledger, destination)
        self.assertEqual(len(saved['sha256']), 64)
        restored = Path(self.tmp.name) / 'restored.db'
        result = recovery.restore(destination, restored, saved['sha256'])
        self.assertTrue(result['restored'])
        with Ledger(restored) as ledger:
            self.assertEqual(Materializer(ledger).report('report'), first)
            self.assertTrue(recovery.audit(ledger)['valid'])
        rebuilt = Path(self.tmp.name) / 'rebuilt.db'
        self.assertTrue(recovery.replay_batches(self.ledger, rebuilt)['cached_reports_and_publications_not_copied'])
        with Ledger(rebuilt) as ledger:
            self.assertEqual(ledger.document().fingerprint, self.ledger.document().fingerprint)
            with self.assertRaises(KeyError):
                Materializer(ledger).report('report')
        with self.assertRaises(FileExistsError):
            recovery.backup(self.ledger, destination)
        with self.assertRaises(ValueError):
            recovery.restore(destination, Path(self.tmp.name) / 'bad.db', '0' * 64)
        self.assertFalse((Path(self.tmp.name) / 'bad.db').exists())


if __name__ == '__main__':
    unittest.main()
