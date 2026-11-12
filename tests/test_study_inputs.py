import json
import unittest
from pathlib import Path


class StudyInputTests(unittest.TestCase):
    def test_domain_references_and_units(self):
        data=json.loads((Path(__file__).resolve().parents[1]/'fixtures/study-input.json').read_text())
        from dispatch_log.schema import Dataset
        dataset=Dataset(data['document'])
        from dispatch_log.replay import replay_visit
        from dispatch_log.selection import select
        for pair in {b['pair'] for b in data['blocks']}:
            rows=[b for b in data['blocks'] if b['pair']==pair]
            self.assertEqual(len(rows),2)
            self.assertEqual({r['assigned'] for r in rows},{'A','B'})
            self.assertEqual(rows[0]['end'],rows[1]['start'])
        self.assertEqual(len(dataset.visits),64)
