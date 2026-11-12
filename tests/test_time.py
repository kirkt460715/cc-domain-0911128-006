import copy
import json
from pathlib import Path
from fractions import Fraction
import random
import unittest
from dispatch_log.schema import Dataset, Invalid, instant, utc, digest
from dispatch_log.replay import replay
from worktime.intervals import union, intersect, subtract, duration, split_days, boundaries, overlap_segments, fair_overlap_credit
from worktime.calendar import worker_availability, daily_availability, shift_quality
from worktime.attribution import visit_attribution, worker_conflicts, daily_worker_totals
from worktime.productivity import service_credit, service_totals, daily_overtime_exposure, exact
from worktime.balance import phase_balance
from worktime.audit import inspect
from operations.pipeline import analyze, audit


class TimeTests(unittest.TestCase):
    def setUp(self):
        self.data = json.loads(Path('fixtures/visits.json').read_text())
        self.dataset = Dataset(self.data)
        self.start = '2026-11-10T00:00:00Z'
        self.end = '2026-11-12T00:00:00Z'
        self.knowledge = '2026-11-12T12:00:00Z'
        self.result = analyze(self.dataset, self.start, self.end, self.end, self.knowledge)

    def test_timestamp_offsets_and_bounds(self):
        self.assertEqual(instant('2026-11-10T08:00:00+08:00', 't'), instant(self.start, 't'))
        self.assertEqual(utc(instant(self.start, 't')), self.start)
        self.assertEqual(instant('1970-01-01T00:00:00Z', 't'), 0)
        with self.assertRaises(Invalid):
            instant('2026-11-10T08:00:00', 't')
        with self.assertRaises(Invalid):
            instant('2026-11-10T08:00:00.1Z', 't')
        with self.assertRaises(Invalid):
            instant('1969-12-31T23:59:59Z', 't')
        with self.assertRaises(Invalid):
            instant('not-time', 't')

    def test_interval_boundary_arithmetic(self):
        self.assertEqual(union([(0, 10), (10, 20), (5, 12), (30, 40)]), [(0, 20), (30, 40)])
        self.assertEqual(intersect([(0, 10)], [(10, 20)]), [])
        self.assertEqual(intersect([(0, 20)], [(5, 10), (15, 30)]), [(5, 10), (15, 20)])
        self.assertEqual(subtract([(0, 20)], [(5, 10), (15, 30)]), [(0, 5), (10, 15)])
        self.assertEqual(subtract([(0, 20)], [(0, 20)]), [])
        self.assertEqual(duration([(0, 20), (5, 10)]), 20)
        self.assertEqual(subtract([(0, 5)], [(8, 9)]), [(0, 5)])
        with self.assertRaises(ValueError):
            union([(1, 1)])
        with self.assertRaises(ValueError):
            union([(True, 10)])
        with self.assertRaises(ValueError):
            union([(20, 10)])

    def test_interval_operations_against_integer_second_sets(self):
        rng = random.Random(112)
        for trial in range(200):
            a = []
            b = []
            for collection in (a, b):
                for _ in range(rng.randrange(10)):
                    start = rng.randrange(30)
                    collection.append((start, start + rng.randrange(1, 15)))
            first = {second for left, right in a for second in range(left, right)}
            last = {second for left, right in b for second in range(left, right)}
            def expanded(intervals):
                return {second for left, right in intervals for second in range(left, right)}
            self.assertEqual(expanded(union(a)), first)
            self.assertEqual(expanded(intersect(a, b)), first & last)
            self.assertEqual(expanded(subtract(a, b)), first - last)
            self.assertEqual(duration(a), len(first))
            self.assertEqual(duration(intersect(a, b)) + duration(subtract(a, b)), duration(a))

    def test_daily_split_timezone_and_cross_midnight(self):
        left = instant('2026-11-10T23:00:00Z', 't')
        right = instant('2026-11-11T01:00:00Z', 't')
        rows = split_days([(left, right)])
        self.assertEqual([row['day'] for row in rows], ['2026-11-10', '2026-11-11'])
        self.assertEqual([row['seconds'] for row in rows], [3600, 3600])
        local = split_days([(left, right)], 480)
        self.assertEqual(len(local), 1)
        self.assertEqual(local[0]['day'], '2026-11-11')
        self.assertEqual(local[0]['seconds'], 7200)
        self.assertEqual(sum(row['seconds'] for row in boundaries(left, right)), 7200)
        with self.assertRaises(ValueError):
            boundaries(right, left)
        with self.assertRaises(Invalid):
            split_days([(left, right)], 900)

    def test_overlaps_and_fair_credit(self):
        labels = {'a': [(0, 10)], 'b': [(5, 15)], 'c': [(10, 12)]}
        overlaps = overlap_segments(labels)
        self.assertEqual(overlaps, [{'start': 5, 'end': 10, 'seconds': 5, 'ids': ['a', 'b']}, {'start': 10, 'end': 12, 'seconds': 2, 'ids': ['b', 'c']}])
        credit = fair_overlap_credit(labels)
        self.assertEqual(exact(credit['credits']['a']), Fraction(15, 2))
        self.assertEqual(exact(credit['credits']['b']), Fraction(13, 2))
        self.assertEqual(exact(credit['credits']['c']), 1)
        self.assertEqual(sum(exact(value) for value in credit['credits'].values()), 15)
        self.assertTrue(credit['not_payroll_or_validity_override'])
        touching = overlap_segments({'a': [(0, 5)], 'b': [(5, 10)]})
        self.assertEqual(touching, [])

    def test_shift_break_union(self):
        first = instant(self.start, 'start')
        end = instant(self.end, 'end')
        availability = worker_availability(self.dataset, first, end)
        self.assertEqual(availability['w1']['shift_seconds'], 9 * 3600)
        self.assertEqual(availability['w1']['break_seconds'], 3600)
        self.assertEqual(availability['w1']['available_seconds'], 8 * 3600)
        self.assertEqual(availability['w2']['available_seconds'], 8 * 3600)
        daily = daily_availability(self.dataset, first, end)
        night = [row for row in daily if row['worker'] == 'w2']
        self.assertEqual([row['available_seconds'] for row in night], [7200, 21600])
        self.assertEqual(shift_quality(self.dataset)['findings'], [])
        data = copy.deepcopy(self.data)
        data['shifts'].append({**data['shifts'][0], 'id': 'duplicate-window'})
        data['shifts'][0]['breaks'].append({'start': '2026-11-10T12:30:00Z', 'end': '2026-11-10T13:00:00Z'})
        duplicated = Dataset(data)
        self.assertEqual(worker_availability(duplicated, first, end)['w1']['available_seconds'], 8 * 3600)
        self.assertTrue(shift_quality(duplicated)['findings'])

    def test_pause_is_excluded_from_active_time(self):
        timeline = next(row for row in self.result['timeline']['visits'] if row['visit'] == 'v1')
        self.assertEqual(timeline['active_seconds'], 3600)
        self.assertEqual(timeline['pause_seconds'], 600)
        self.assertEqual(timeline['travel_seconds'], 1800)
        self.assertEqual(timeline['state'], 'completed')
        self.assertTrue(timeline['valid'])
        attribution = next(row for row in self.result['attributions'] if row['visit'] == 'retry')
        self.assertEqual(attribution['active_seconds'], 3600)
        self.assertEqual(attribution['active_in_available_shift_seconds'], 0)
        self.assertEqual(attribution['active_outside_available_shift_seconds'], 3600)
        self.assertTrue(inspect(self.dataset, self.result)['valid'])

    def test_worker_union_reduces_double_booking(self):
        conflict = next(row for row in self.result['worker_conflicts'] if row['worker'] == 'w1')
        self.assertEqual(conflict['sum_visit_busy_seconds'], 9600)
        self.assertEqual(conflict['union_busy_seconds'], 7800)
        self.assertEqual(conflict['duplicate_attributed_seconds'], 1800)
        self.assertEqual(conflict['conflict_seconds'], 1800)
        self.assertEqual(conflict['visits_in_conflict'], ['v1', 'v2'])
        daily = next(row for row in self.result['daily_workers'] if row['worker'] == 'w1' and row['day'] == '2026-11-10')
        self.assertEqual(daily['busy_union_seconds'], 7800)
        self.assertEqual(daily['busy_in_shift_seconds'], 7800)
        self.assertEqual(daily['available_seconds'], 28800)
        self.assertEqual(daily['idle_available_seconds'], 21000)
        self.assertTrue(audit(self.dataset, self.result, replay_all=True)['valid'])

    def test_service_credit_and_phase_partition(self):
        credit = service_credit(self.dataset, self.result)
        self.assertTrue(credit['review_allocation_not_payroll_or_resolution_of_conflicts'])
        total = sum(exact(row['busy_credit_seconds']) for row in credit['rows'])
        physical = sum(row['busy_union_seconds'] for row in self.result['daily_workers'])
        self.assertEqual(total, physical)
        for row in credit['worker_checks']:
            self.assertEqual(exact(row['credited_seconds']), row['physical_busy_seconds'])
        services = service_totals(self.dataset, self.result)
        self.assertEqual(len(services['rows']), 1)
        self.assertEqual(services['rows'][0]['completed_in_sla_cohort'], 4)
        phases = phase_balance(self.dataset, self.result)
        for row in phases:
            self.assertEqual(row['active_without_travel_seconds'] + row['travel_without_active_seconds'] + row['active_and_travel_overlap_seconds'], row['busy_union_seconds'])
        w1 = next(row for row in phases if row['worker'] == 'w1' and row['day'] == '2026-11-10')
        self.assertEqual(w1['active_and_travel_overlap_seconds'], 1200)
        overtime = daily_overtime_exposure(self.dataset, self.result)
        self.assertTrue(any(row['review_required'] for row in overtime))

    def test_night_active_time_and_worktime_clipping(self):
        night = next(row for row in self.result['timeline']['visits'] if row['visit'] == 'night')
        self.assertEqual(night['active_seconds'], 7200)
        daily = [row for row in self.result['daily_workers'] if row['worker'] == 'w2']
        self.assertEqual(daily[1]['active_union_seconds'], 3600)
        clipped = analyze(self.dataset, '2026-11-11T00:00:00Z', self.end, self.end, self.knowledge)
        attribution = next(row for row in clipped['attributions'] if row['visit'] == 'night')
        self.assertEqual(attribution['active_seconds'], 3600)
        self.assertEqual(attribution['travel_seconds'], 0)
        self.assertEqual(clipped['sla_overall'], self.result['sla_overall'])
        self.assertIn('SLA_cohort_all_requests', clipped['scope'])
        with self.assertRaises(ValueError):
            analyze(self.dataset, self.start, self.end, '2026-11-11T00:00:00Z', self.knowledge)


if __name__ == '__main__':
    unittest.main()
