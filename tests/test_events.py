import copy
import json
from pathlib import Path
import unittest
from dispatch_log.schema import Dataset, Invalid, digest, canonical, instant
from dispatch_log.selection import select, affected_visits
from dispatch_log.replay import replay
from quality.sla import classify, summarize
from quality.retries import chains, retry_summary
from quality.waits import waiting_observations, survival, restricted_mean
from quality.latency import distribution, event_latency, revision_churn, lateness_buckets
from quality.evidence import visit_evidence, revision_impact, completeness, timing_anomalies
from operations.pipeline import analyze, audit


class EventTests(unittest.TestCase):
    def setUp(self):
        self.data = json.loads(Path('fixtures/visits.json').read_text())
        self.start = '2026-11-10T00:00:00Z'
        self.end = '2026-11-12T00:00:00Z'
        self.known = '2026-11-12T12:00:00Z'
        self.dataset = Dataset(self.data)
        self.result = analyze(self.dataset, self.start, self.end, self.end, self.known)

    def timeline(self, data, visit='v1', effective=None, known=None):
        output = replay(Dataset(data), effective or self.end, known or self.known)
        return next(row for row in output['visits'] if row['visit'] == visit)

    def test_input_is_owned_and_export_is_detached(self):
        before = self.dataset.fingerprint
        exported = self.dataset.export()
        exported['workers'][0]['region'] = 'changed'
        self.data['services'][0]['response_seconds'] = 100
        self.assertEqual(self.dataset.fingerprint, before)
        self.assertEqual(self.dataset.services['care']['response_seconds'], 1800)
        self.assertEqual(self.dataset.workers['w1']['region'], 'east')
        self.assertNotEqual(digest(exported), before)
        self.assertEqual(digest(self.dataset.export()), before)

    def test_strict_input_schema_and_types(self):
        for key, value in [('version', True), ('version', 2), ('services', {}), ('events', None)]:
            with self.subTest(key=key, value=value):
                bad = copy.deepcopy(self.data)
                bad[key] = value
                with self.assertRaises(Invalid):
                    Dataset(bad)
        bad = copy.deepcopy(self.data)
        bad['unexpected'] = []
        with self.assertRaises(Invalid):
            Dataset(bad)
        del bad['unexpected']
        del bad['workers']
        with self.assertRaises(Invalid):
            Dataset(bad)

    def test_service_worker_and_reference_validation(self):
        for table, key, value in [('services', 'response_seconds', True), ('services', 'maximum_active_seconds', 1), ('workers', 'daily_regular_seconds', 86401), ('workers', 'id', ''), ('workers', 'region', 'bad\nregion'), ('visits', 'service', 'unknown'), ('shifts', 'worker', 'unknown'), ('events', 'worker', 'unknown')]:
            with self.subTest(table=table, key=key):
                bad = copy.deepcopy(self.data)
                bad[table][0][key] = value
                with self.assertRaises(Invalid):
                    Dataset(bad)
        bad = copy.deepcopy(self.data)
        bad['workers'].append(copy.deepcopy(bad['workers'][0]))
        with self.assertRaises(Invalid):
            Dataset(bad)
        self.assertEqual(len(self.dataset.workers), 2)

    def test_retry_link_constraints(self):
        for key, value in [('parent', 'missing'), ('parent', 'retry'), ('client', 'different'), ('service', 'missing'), ('requested_at', '2026-11-10T09:00:00Z')]:
            with self.subTest(key=key):
                bad = copy.deepcopy(self.data)
                bad['visits'][3][key] = value
                with self.assertRaises(Invalid):
                    Dataset(bad)
        self.assertEqual(self.dataset.visits['retry']['parent'], 'cancel')
        bad = copy.deepcopy(self.data)
        bad['visits'][0]['promised_start'] = '2026-11-10T07:00:00Z'
        with self.assertRaises(Invalid):
            Dataset(bad)

    def test_shift_bounds_and_breaks(self):
        bad = copy.deepcopy(self.data)
        bad['shifts'][0]['end'] = bad['shifts'][0]['start']
        with self.assertRaises(Invalid):
            Dataset(bad)
        bad = copy.deepcopy(self.data)
        bad['shifts'][0]['end'] = '2026-11-18T08:00:00Z'
        with self.assertRaises(Invalid):
            Dataset(bad)
        bad = copy.deepcopy(self.data)
        bad['shifts'][0]['breaks'][0]['end'] = '2026-11-10T18:00:00Z'
        with self.assertRaises(Invalid):
            Dataset(bad)
        bad['shifts'][0]['breaks'] = []
        self.assertEqual(len(Dataset(bad).shifts), 2)

    def test_event_contract_rejects_unobserved_future_and_invalid_reason(self):
        for key, value in [('revision', 0), ('sequence', True), ('kind', 'invented'), ('void', 1), ('observed_at', self.start), ('visit', 'unknown')]:
            with self.subTest(key=key):
                bad = copy.deepcopy(self.data)
                bad['events'][0][key] = value
                with self.assertRaises(Invalid):
                    Dataset(bad)
        bad = copy.deepcopy(self.data)
        bad['events'][4]['reason'] = 'unknown-break'
        with self.assertRaises(Invalid):
            Dataset(bad)
        bad = copy.deepcopy(self.data)
        bad['events'][12]['reason'] = None
        with self.assertRaises(Invalid):
            Dataset(bad)
        bad = copy.deepcopy(self.data)
        bad['events'].append(copy.deepcopy(bad['events'][0]))
        with self.assertRaises(Invalid):
            Dataset(bad)

    def test_out_of_order_arrival_is_replayed_by_sequence(self):
        shuffled = copy.deepcopy(self.data)
        shuffled['events'].reverse()
        before = self.timeline(self.data)
        after = self.timeline(shuffled)
        self.assertEqual(before, after)
        self.assertTrue(after['valid'])
        self.assertEqual(after['state'], 'completed')
        self.assertEqual(after['active_seconds'], 3600)
        self.assertEqual(after['pause_seconds'], 600)
        self.assertEqual(len(after['selected_events']), 7)

    def test_revision_selects_by_knowledge_without_future_leakage(self):
        changed = copy.deepcopy(self.data)
        event = next(row for row in changed['events'] if row['id'] == 'v1-7')
        correction = {**event, 'revision': 2, 'observed_at': '2026-11-12T13:00:00Z', 'effective_at': '2026-11-10T10:30:00Z'}
        changed['events'].append(correction)
        old = self.timeline(changed)
        new = self.timeline(changed, known='2026-11-12T14:00:00Z')
        self.assertEqual(old['active_seconds'], 3600)
        self.assertEqual(new['active_seconds'], 4800)
        self.assertEqual(new['state'], 'completed')
        self.assertTrue(new['valid'])
        impact = revision_impact(Dataset(changed), self.known, '2026-11-12T14:00:00Z')
        self.assertEqual(impact['affected_visits'], ['v1'])
        self.assertEqual(impact['rows'][0]['before_revision'], 1)
        self.assertEqual(impact['rows'][0]['after_revision'], 2)
        self.assertIn('effective_at', impact['rows'][0]['changed_fields'])

    def test_void_terminal_restores_open_state(self):
        changed = copy.deepcopy(self.data)
        event = next(row for row in changed['events'] if row['id'] == 'v1-7')
        changed['events'].append({**event, 'revision': 2, 'observed_at': '2026-11-11T12:00:00Z', 'void': True})
        timeline = self.timeline(changed)
        self.assertTrue(timeline['valid'])
        self.assertEqual(timeline['state'], 'active')
        self.assertTrue(timeline['right_censored'])
        self.assertNotIn('completed', timeline['milestones'])
        self.assertEqual(len(timeline['selected_events']), 6)
        selection = select(changed['events'], self.known)
        self.assertIn({'event': 'v1-7', 'reason': 'voided', 'revision': 2}, selection['omitted'])
        impact = affected_visits(select(self.data['events'], self.known), selection)
        self.assertEqual(impact['visits'], ['v1'])

    def test_gap_and_nonincreasing_revision_history_are_findings(self):
        changed = copy.deepcopy(self.data)
        event = changed['events'][0]
        changed['events'].append({**event, 'revision': 3, 'observed_at': '2026-11-11T00:00:00Z'})
        timeline = self.timeline(changed)
        self.assertFalse(timeline['valid'])
        self.assertIn('revision_gap', [row['code'] for row in timeline['issues']])
        changed['events'][-1]['revision'] = 2
        changed['events'][-1]['observed_at'] = event['observed_at']
        timeline = self.timeline(changed)
        self.assertFalse(timeline['valid'])
        self.assertIn('nonincreasing_revision_observation', [row['code'] for row in timeline['issues']])
        self.assertNotEqual(timeline['state'], 'completed')

    def test_sequence_gap_and_worker_change_quarantine_timeline(self):
        changed = copy.deepcopy(self.data)
        changed['events'][1]['sequence'] = 20
        timeline = self.timeline(changed)
        self.assertFalse(timeline['valid'])
        self.assertIn('sequence_gap_or_collision', [row['code'] for row in timeline['issues']])
        changed = copy.deepcopy(self.data)
        changed['events'][3]['worker'] = 'w2'
        timeline = self.timeline(changed)
        self.assertFalse(timeline['valid'])
        self.assertIn('worker_changes_without_new_visit', [row['code'] for row in timeline['issues']])
        result = analyze(Dataset(changed), self.start, self.end, self.end, self.known)
        row = next(row for row in result['sla_visits'] if row['visit'] == 'v1')
        self.assertFalse(row['duration_denominator'])
        self.assertIsNone(row['duration_met'])
        self.assertTrue(audit(Dataset(changed), result, True)['valid'])

    def test_time_reversal_and_invalid_transition_are_reported(self):
        changed = copy.deepcopy(self.data)
        changed['events'][3]['effective_at'] = '2026-11-10T08:40:00Z'
        timeline = self.timeline(changed)
        self.assertFalse(timeline['valid'])
        self.assertIn('event_time_goes_backwards', [row['code'] for row in timeline['issues']])
        changed = copy.deepcopy(self.data)
        changed['events'][0]['kind'] = 'completed'
        timeline = self.timeline(changed)
        self.assertFalse(timeline['valid'])
        self.assertIn('invalid_transition', [row['code'] for row in timeline['issues']])
        self.assertNotIn('accepted', timeline['milestones'])
        self.assertEqual(self.timeline(self.data)['state'], 'completed')

    def test_business_cutoff_keeps_open_active_interval(self):
        timeline = self.timeline(self.data, effective='2026-11-10T09:20:00Z')
        self.assertEqual(timeline['state'], 'active')
        self.assertEqual(timeline['active_seconds'], 1200)
        self.assertEqual(timeline['pause_seconds'], 0)
        self.assertTrue(timeline['right_censored'])
        self.assertEqual(len(timeline['selected_events']), 4)
        future = self.timeline(self.data, visit='night', effective='2026-11-10T09:20:00Z')
        self.assertEqual(future['state'], 'not_requested')
        self.assertEqual(future['selected_events'], [])
        self.assertTrue(future['valid'])
        with self.assertRaises(ValueError):
            replay(self.dataset, self.end, self.start)

    def test_cancellation_response_and_retry_denominators(self):
        rows = {row['visit']: row for row in self.result['sla_visits']}
        self.assertFalse(rows['cancel']['response_denominator'])
        self.assertFalse(rows['cancel']['start_denominator'])
        self.assertFalse(rows['cancel']['duration_denominator'])
        self.assertTrue(rows['cancel']['cancellation_before_acceptance_excluded_from_response'])
        self.assertTrue(rows['retry']['response_met'])
        self.assertTrue(rows['retry']['start_met'])
        self.assertTrue(rows['retry']['duration_met'])
        self.assertFalse(rows['open']['response_met'])
        self.assertFalse(rows['open']['start_met'])
        summary = self.result['sla_overall']['rows'][0]
        self.assertEqual(summary['requested_visits'], 6)
        self.assertEqual(summary['completed'], 4)
        self.assertEqual(summary['cancelled'], 1)
        self.assertEqual(summary['censored'], 1)
        self.assertEqual(summary['sla']['response']['denominator'], 5)
        self.assertEqual(summary['sla']['response']['numerator'], 4)
        self.assertEqual(summary['sla']['duration']['denominator'], 4)
        retry = self.result['retry_summary']
        self.assertEqual(retry['original_needs'], 5)
        self.assertEqual(retry['attempts'], 6)
        self.assertEqual(retry['retried_needs'], 1)
        self.assertEqual(retry['completed_original_needs'], 4)

    def test_future_requests_not_in_sla_or_waiting_risk_sets(self):
        cutoff = '2026-11-10T09:00:00Z'
        timeline = replay(self.dataset, cutoff, self.known)
        rows = classify(self.dataset, timeline)
        grouped = summarize(rows)['rows'][0]
        self.assertEqual(grouped['requested_visits'], 2)
        self.assertEqual(sum(row['future_request'] for row in rows), 4)
        waiting = waiting_observations(self.dataset, timeline['visits'], cutoff)
        self.assertEqual(len(waiting['rows']), 2)
        self.assertEqual(waiting['excluded'], [])
        self.assertEqual({row['visit'] for row in waiting['rows']}, {'v1', 'v2'})
        with self.assertRaises(ValueError):
            summarize(rows, ['region', 'region'])
        with self.assertRaises(ValueError):
            summarize(rows, ['client'])

    def test_waiting_cancellations_excluded_and_open_right_censored(self):
        waiting = waiting_observations(self.dataset, self.result['timeline']['visits'], self.end)
        self.assertEqual(waiting['excluded'], [{'visit': 'cancel', 'reason': 'cancelled_before_target', 'cancelled_at': instant('2026-11-10T10:20:00Z', 't')}])
        rows = {row['visit']: row for row in waiting['rows']}
        self.assertTrue(rows['v1']['event_observed'])
        self.assertEqual(rows['v1']['seconds'], 3600)
        self.assertFalse(rows['open']['event_observed'])
        estimate = survival(waiting)
        self.assertEqual(estimate['observations'], 5)
        self.assertEqual(estimate['median_seconds'], 3600)
        self.assertEqual(estimate['excluded_cancelled_or_invalid'], 1)
        self.assertEqual(estimate['rows'][0]['events'], 4)
        self.assertEqual(estimate['rows'][0]['survival'], {'numerator': 1, 'denominator': 5})
        self.assertEqual(estimate['rows'][-1]['censored'], 1)
        with self.assertRaises(ValueError):
            waiting_observations(self.dataset, [], self.end, 'arrived')

    def test_survival_tie_order_empty_and_all_censored(self):
        data = {'target': 'started', 'rows': [{'seconds': 10, 'event_observed': True}, {'seconds': 10, 'event_observed': False}, {'seconds': 20, 'event_observed': True}], 'excluded': []}
        result = survival(data)
        self.assertEqual(result['rows'][0]['at_risk'], 3)
        self.assertEqual(result['rows'][0]['survival'], {'numerator': 2, 'denominator': 3})
        self.assertEqual(result['rows'][1]['at_risk'], 1)
        self.assertEqual(result['median_seconds'], 20)
        data['rows'] = [{'seconds': 10, 'event_observed': False}]
        result = survival(data)
        self.assertIsNone(result['median_seconds'])
        self.assertEqual(result['rows'][0]['survival'], {'numerator': 1, 'denominator': 1})
        data['rows'] = []
        result = survival(data)
        self.assertIsNone(result['median_seconds'])
        self.assertEqual(result['rows'], [])
        self.assertEqual(result['observations'], 0)

    def test_latency_counts_every_known_revision(self):
        self.assertEqual(distribution([])['count'], 0)
        self.assertIsNone(distribution([])['median'])
        self.assertEqual(distribution([1, 10, 3, 2])['median'], 2)
        self.assertEqual(distribution([1, 10, 3, 2])['p90'], 10)
        latency = event_latency(self.dataset, self.known)
        self.assertEqual(sum(row['latency_seconds']['count'] for row in latency['rows']), 24)
        self.assertEqual(sum(row['latency_seconds']['sum'] for row in latency['rows']), 0)
        buckets = lateness_buckets(self.dataset, self.known)
        self.assertEqual([row['known_revisions'] for row in buckets], [24, 0, 0, 0])
        self.assertEqual(len(revision_churn(self.dataset, self.known)), 24)
        with self.assertRaises(ValueError):
            lateness_buckets(self.dataset, self.known, [10, 10])
        with self.assertRaises(ValueError):
            lateness_buckets(self.dataset, self.known, [20, 10])

    def test_evidence_completeness_and_late_correction(self):
        changed = copy.deepcopy(self.data)
        changed['events'].append({**changed['events'][0], 'revision': 2, 'observed_at': '2026-11-12T13:00:00Z'})
        dataset = Dataset(changed)
        result = analyze(dataset, self.start, self.end, self.end, self.known)
        evidence = visit_evidence(dataset, result, 'v1')
        hidden = next(row for row in evidence['events'] if row['revision'] == 2)
        self.assertFalse(hidden['known_at_cutoff'])
        self.assertIsNone(hidden['body'])
        self.assertFalse(hidden['selected_revision'])
        self.assertEqual(evidence['missing_standard_milestones'], [])
        complete = completeness(result)
        self.assertEqual(sum(row['visits'] for row in complete['rows']), 6)
        self.assertEqual(timing_anomalies(dataset, result)['rows'], [])
        later = analyze(dataset, self.start, self.end, self.end, '2026-11-12T14:00:00Z')
        anomalies = timing_anomalies(dataset, later)
        self.assertEqual(len(anomalies['rows']), 1)
        self.assertEqual(anomalies['rows'][0]['codes'], ['late_observation'])
        self.assertEqual(anomalies['rows'][0]['revision'], 2)
        with self.assertRaises(KeyError):
            visit_evidence(dataset, result, 'unknown')
        with self.assertRaises(ValueError):
            revision_impact(dataset, self.known, self.known)


if __name__ == '__main__':
    unittest.main()
