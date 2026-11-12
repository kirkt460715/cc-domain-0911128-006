import copy
import hashlib
import http.client
from http.server import HTTPServer
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from dispatch_log.schema import Dataset, digest, canonical
from dispatch_log.ledger import Ledger, Conflict
from dispatch_log import tabular
from operations.pipeline import analyze, audit
from operations.materializer import Materializer
from operations.publication import Publisher, safe_sla, verify
from operations.query import visits, detail, report, encode, decode
from operations import bundle
from operations.http_api import handler
from quality.cases import Cases


class DeliveryTests(unittest.TestCase):
    def setUp(self):
        self.data = json.loads(Path('fixtures/visits.json').read_text())
        self.dataset = Dataset(self.data)
        self.start = '2026-11-10T00:00:00Z'
        self.end = '2026-11-12T00:00:00Z'
        self.known = '2026-11-12T12:00:00Z'
        self.result = analyze(self.dataset, self.start, self.end, self.end, self.known)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def service(self):
        ledger = Ledger(self.root / 'visits.db')
        self.addCleanup(ledger.close)
        ledger.import_document('initial', self.data)
        materializer = Materializer(ledger)
        materializer.produce('report', self.start, self.end, self.end, self.known)
        return materializer

    def test_query_pagination_is_complete_and_stable(self):
        first = visits(self.result, columns=['visit', 'state'], limit=2)
        self.assertEqual(first['total'], 6)
        self.assertEqual([row['visit'] for row in first['rows']], ['cancel', 'night'])
        self.assertIsNotNone(first['next_cursor'])
        second = visits(self.result, columns=['visit', 'state'], limit=2, cursor=first['next_cursor'])
        third = visits(self.result, columns=['visit', 'state'], limit=2, cursor=second['next_cursor'])
        self.assertIsNone(third['next_cursor'])
        all_rows = first['rows'] + second['rows'] + third['rows']
        self.assertEqual([row['visit'] for row in all_rows], ['cancel', 'night', 'open', 'retry', 'v1', 'v2'])
        self.assertEqual(len({row['visit'] for row in all_rows}), 6)
        self.assertEqual(set(all_rows[0]), {'visit', 'state'})

    def test_query_cursor_binds_result_filter_and_columns(self):
        first = visits(self.result, limit=1)
        with self.assertRaises(ValueError):
            visits(self.result, columns=['visit'], cursor=first['next_cursor'])
        with self.assertRaises(ValueError):
            visits(self.result, filters={'region': ['east']}, cursor=first['next_cursor'])
        changed = copy.deepcopy(self.result)
        changed['result_digest'] = 'changed'
        with self.assertRaises(ValueError):
            visits(changed, cursor=first['next_cursor'])
        with self.assertRaises(ValueError):
            visits(self.result, cursor=encode({'context': 'wrong', 'offset': 100}))
        with self.assertRaises(ValueError):
            decode('!invalid!')
        with self.assertRaises(ValueError):
            decode(encode([]))

    def test_query_input_validation_and_exact_boolean_filters(self):
        rows = visits(self.result, filters={'valid': [True], 'region': ['east']}, columns=['visit'])
        self.assertEqual(rows['rows'], [{'visit': 'v1'}, {'visit': 'v2'}])
        self.assertEqual(visits(self.result, filters={'valid': ['True']})['total'], 0)
        self.assertEqual(visits(self.result, filters={'region': []})['rows'], [])
        for options in [{'limit': 0}, {'limit': True}, {'columns': []}, {'columns': ['visit', 'visit']}, {'columns': ['secret']}, {'filters': {'worker': ['w1']}}, {'filters': {'valid': [1]}}, {'filters': {'region': ['east', 'east']}}]:
            with self.subTest(options=options):
                with self.assertRaises(ValueError):
                    visits(self.result, **options)
        self.assertEqual(decode(encode({'x': '中文'})), {'x': '中文'})
        self.assertEqual(detail(self.dataset, self.result, 'v1')['timeline']['state'], 'completed')
        with self.assertRaises(KeyError):
            detail(self.dataset, self.result, 'unknown')

    def test_report_dispatch_preserves_business_outputs(self):
        self.assertEqual(report(self.dataset, self.result, 'sla'), self.result['sla_overall'])
        self.assertEqual(report(self.dataset, self.result, 'conflicts'), self.result['worker_conflicts'])
        self.assertEqual(report(self.dataset, self.result, 'retry-summary')['original_needs'], 5)
        self.assertTrue(report(self.dataset, self.result, 'interval-audit')['valid'])
        self.assertTrue(report(self.dataset, self.result, 'metrics-audit')['valid'])
        self.assertEqual(report(self.dataset, self.result, 'visits', {'filters': {'state': ['completed']}})['total'], 4)
        self.assertEqual(report(self.dataset, self.result, 'visit', {'id': 'cancel'})['sla']['cancellation_reason'], 'provider')
        self.assertEqual(report(self.dataset, self.result, 'evidence', {'visit': 'open'})['state'], 'accepted')
        for kind in ('daily-workers', 'availability', 'quality', 'retry-chains', 'request-cohorts', 'completion-cohorts', 'waiting-by-region', 'latency', 'revision-churn', 'latency-buckets', 'service-credit', 'service-totals', 'phase-balance', 'overtime', 'backlog', 'backlog-summary', 'triage', 'cancellations', 'denominators', 'completeness', 'timing-anomalies'):
            with self.subTest(kind=kind):
                self.assertIsNotNone(report(self.dataset, self.result, kind))
        with self.assertRaises(ValueError):
            report(self.dataset, self.result, 'unknown')
        with self.assertRaises(ValueError):
            report(self.dataset, self.result, 'sla', {'unexpected': True})

    def test_publication_suppresses_partition_and_complement(self):
        packet = safe_sla(self.result, minimum_clients=3)
        self.assertTrue(verify(packet))
        self.assertEqual(len(packet['rows']), 2)
        self.assertTrue(all(row['suppressed'] for row in packet['rows']))
        self.assertIsNone(packet['overall'])
        self.assertFalse(packet['contains_visit_or_worker_ids'])
        self.assertTrue(all(set(row) == {'region', 'suppressed'} for row in packet['rows']))
        open_packet = safe_sla(self.result, minimum_clients=2)
        self.assertTrue(verify(open_packet))
        self.assertFalse(any(row['suppressed'] for row in open_packet['rows']))
        self.assertEqual(open_packet['overall'], self.result['sla_overall'])
        self.assertEqual([row['distinct_clients'] for row in open_packet['rows']], [2, 3])
        self.assertNotIn('client-v1', canonical(open_packet))
        self.assertNotIn('worker', canonical(open_packet['rows']))
        with self.assertRaises(ValueError):
            safe_sla(self.result, dimension='client')
        with self.assertRaises(ValueError):
            safe_sla(self.result, minimum_clients=0)

    def test_publication_verifier_rejects_measures_in_suppressed_cells(self):
        packet = safe_sla(self.result)
        packet['rows'][0]['requested_visits'] = 2
        packet['publication_digest'] = digest({key: value for key, value in packet.items() if key != 'publication_digest'})
        with self.assertRaisesRegex(ValueError, 'suppressed row'):
            verify(packet)
        packet = safe_sla(self.result)
        packet['overall'] = self.result['sla_overall']
        packet['publication_digest'] = digest({key: value for key, value in packet.items() if key != 'publication_digest'})
        with self.assertRaisesRegex(ValueError, 'total reveals'):
            verify(packet)
        packet = safe_sla(self.result)
        packet['dimension'] = 'service'
        with self.assertRaisesRegex(ValueError, 'checksum'):
            verify(packet)

    def test_published_label_cannot_be_rebound(self):
        materializer = self.service()
        publisher = Publisher(materializer)
        first = publisher.publish('release', 'report')
        self.assertEqual(publisher.publish('release', 'report'), first)
        self.assertEqual(publisher.get('release'), first)
        with self.assertRaises(Conflict):
            publisher.publish('release', 'report', minimum_clients=1)
        materializer.produce('second', self.start, self.end, self.end, self.known)
        with self.assertRaises(Conflict):
            publisher.publish('release', 'second')
        self.assertEqual(publisher.audit()['publications'], 1)
        self.assertEqual(publisher.get('release'), first)
        with self.assertRaises(KeyError):
            publisher.get('unknown')

    def test_bundle_is_replayable_and_rejects_tamper(self):
        directory = self.root / 'bundle'
        result = bundle.write(self.dataset, self.result, directory)
        self.assertTrue(result['verified'])
        self.assertEqual(bundle.verify(directory), result)
        with self.assertRaises(FileExistsError):
            bundle.write(self.dataset, self.result, directory)
        source = directory / 'result.json'
        original = source.read_bytes()
        source.write_bytes(original + b' ')
        with self.assertRaisesRegex(ValueError, 'checksum'):
            bundle.verify(directory)
        source.write_bytes(original)
        (directory / 'extra.json').write_text('{}')
        with self.assertRaisesRegex(ValueError, 'file set'):
            bundle.verify(directory)
        (directory / 'extra.json').unlink()
        self.assertTrue(bundle.verify(directory)['verified'])

    def test_bundle_rejects_recomputed_manifest_with_false_derived_report(self):
        directory = self.root / 'bundle'
        bundle.write(self.dataset, self.result, directory)
        public = json.loads((directory / 'public.json').read_text())
        public['minimum_clients'] = 1
        body = canonical(public)
        (directory / 'public.json').write_text(body)
        manifest = json.loads((directory / 'manifest.json').read_text())
        manifest['files']['public.json'] = hashlib.sha256(body.encode()).hexdigest()
        (directory / 'manifest.json').write_text(canonical(manifest))
        with self.assertRaisesRegex(ValueError, 'derived content replay'):
            bundle.verify(directory)
        self.assertEqual(len(list(directory.iterdir())), 9)

    def test_csv_roundtrip_preserves_nulls_unicode_and_nested_breaks(self):
        data = copy.deepcopy(self.data)
        data['workers'][0]['region'] = '东区,"甲"'
        dataset = Dataset(data)
        directory = self.root / 'tables'
        result = tabular.write_directory(dataset, directory)
        self.assertEqual(len(result['files']), 5)
        restored = tabular.read_directory(directory)
        self.assertEqual(restored.fingerprint, dataset.fingerprint)
        self.assertEqual(restored.workers['w1']['region'], '东区,"甲"')
        self.assertIsNone(restored.visits['v1']['parent'])
        self.assertEqual(restored.shifts['s1']['breaks'], dataset.shifts['s1']['breaks'])
        self.assertFalse(restored.events[0]['void'])
        with self.assertRaises(FileExistsError):
            tabular.write_directory(dataset, directory)
        (directory / 'extra.csv').write_text('x\n')
        with self.assertRaises(ValueError):
            tabular.read_directory(directory)

    def test_csv_parser_rejects_ambiguous_types_and_structure(self):
        for value in ('1.0', '-1', '１２', '', ' 1', '1e2'):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    tabular.parse(value, 'int')
        self.assertEqual(tabular.parse('001', 'int'), 1)
        self.assertIsNone(tabular.parse('', 'nullable'))
        self.assertFalse(tabular.parse('false', 'bool'))
        with self.assertRaises(ValueError):
            tabular.parse('False', 'bool')
        with self.assertRaises(ValueError):
            tabular.read_table('workers', 'id,id,region\na,b,c\n')
        with self.assertRaises(ValueError):
            tabular.read_table('workers', 'id,region,daily_regular_seconds\nw,east\n')
        with self.assertRaises(ValueError):
            tabular.read_table('unknown', '')
        csv = tabular.write_table(self.dataset, 'workers')
        self.assertEqual(tabular.read_table('workers', csv), self.data['workers'])

    def test_case_requires_immutable_finding_and_separate_reviewer(self):
        materializer = self.service()
        cases = Cases(materializer)
        result = materializer.report('report')['result']
        finding = result['quality']['findings'][0]
        opened = cases.open('case', 'report', digest(finding), 'analyst')
        self.assertEqual(opened['state'], 'open')
        self.assertEqual(opened['revision'], 1)
        self.assertEqual(cases.open('case', 'report', digest(finding), 'analyst'), opened)
        self.assertEqual(len(cases.pending()), 1)
        with self.assertRaises(Conflict):
            cases.open('case', 'report', digest(finding), 'other')
        with self.assertRaises(ValueError):
            cases.open('missing-finding', 'report', '0' * 64, 'analyst')
        with self.assertRaises(Conflict):
            cases.resolve('case', 1, 'report', 'analyst', 'self-review')
        with self.assertRaises(Conflict):
            cases.resolve('case', 2, 'report', 'reviewer', 'stale')
        with self.assertRaises(ValueError):
            cases.resolve('case', 1, 'report', 'reviewer', 'same knowledge')
        materializer.produce('later', self.start, self.end, self.end, '2026-11-12T14:00:00Z')
        with self.assertRaises(ValueError):
            cases.resolve('case', 1, 'later', 'reviewer', 'still present')
        self.assertEqual(cases.get('case'), opened)
        self.assertTrue(cases.audit()['valid'])

    def test_http_authentication_and_snapshot_query(self):
        materializer = self.service()
        token = 'local-synthetic-token-123456789'
        server = HTTPServer(('127.0.0.1', 0), handler(str(self.root / 'visits.db'), token))
        thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .01})
        thread.start()
        def request(path, body, auth=None, content_type='application/json'):
            connection = http.client.HTTPConnection('127.0.0.1', server.server_port, timeout=5)
            headers = {'Content-Type': content_type}
            if auth is not None:
                headers['Authorization'] = 'Bearer ' + auth
            connection.request('POST', path, canonical(body), headers)
            response = connection.getresponse()
            status, result = response.status, json.loads(response.read())
            connection.close()
            return status, result
        try:
            status, denied = request('/read/status', {})
            self.assertEqual(status, 401)
            self.assertEqual(denied['error'], 'unauthorized')
            status, ready = request('/read/status', {}, token)
            self.assertEqual(status, 200)
            self.assertEqual(ready['result']['generation'], 1)
            self.assertEqual(ready['result']['reports'], ['report'])
            status, rows = request('/read/report', {'id': 'report', 'kind': 'visits', 'options': {'columns': ['visit'], 'limit': 1}}, token)
            self.assertEqual(status, 200)
            self.assertEqual(rows['result']['total'], 6)
            self.assertEqual(rows['result']['rows'], [{'visit': 'cancel'}])
            self.assertEqual(request('/read/snapshot', {'id': 'missing'}, token)[0], 404)
            self.assertEqual(request('/write/ingest', {}, token)[0], 404)
            self.assertEqual(request('/read/status?x=1', {}, token)[0], 404)
            self.assertEqual(request('/read/status', {}, token, 'text/plain')[0], 415)
            self.assertEqual(request('/read/report', {'id': 'report', 'kind': 'unknown'}, token)[0], 400)
        finally:
            server.shutdown()
            thread.join(timeout=5)
            server.server_close()
        self.assertFalse(thread.is_alive())
        self.assertEqual(materializer.ledger.generation(), 1)
        with self.assertRaises(ValueError):
            handler(str(self.root / 'visits.db'), 'short')

    def test_cli_demo_uses_local_fixture_and_returns_recovery_evidence(self):
        command = [sys.executable, '-B', 'visit_metrics.py', 'demo', '--fixture', 'fixtures/visits.json']
        result = subprocess.run(command, capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)
        body = json.loads(result.stdout)
        self.assertEqual(body['source'], 'local_synthetic_fixture')
        self.assertFalse(body['runtime_downloads'])
        self.assertTrue(body['backup_restore']['restored'])
        self.assertTrue(body['bundle']['verified'])
        self.assertEqual(body['cache_reuse']['reused_visits'], 6)
        self.assertEqual(body['cache_reuse']['recomputed_visits'], 0)
        bad = subprocess.run([sys.executable, '-B', 'visit_metrics.py', 'analyze', '--input', 'missing.json', '--start', self.start, '--end', self.end, '--effective-at', self.end, '--knowledge-at', self.known], capture_output=True, text=True, timeout=20)
        self.assertEqual(bad.returncode, 2)
        self.assertIn('error', json.loads(bad.stderr))
        self.assertEqual(bad.stdout, '')


if __name__ == '__main__':
    unittest.main()
