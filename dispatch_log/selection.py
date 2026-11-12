from collections import defaultdict
from dispatch_log.schema import instant, digest, validate_events


def select(events, knowledge_at):
    cutoff = instant(knowledge_at, 'knowledge_at')
    versions = defaultdict(list)
    for event in validate_events(events):
        versions[event['id']].append(event)
    selected = []
    omitted = []
    issues = []
    for ident, values in sorted(versions.items()):
        values = [row for row in values if instant(row['observed_at'], 'observed_at') <= cutoff]
        if not values:
            omitted.append({'event': ident, 'reason': 'not_yet_observed'})
            continue
        ordered = sorted(values, key=lambda row: row['revision'])
        revisions = [row['revision'] for row in ordered]
        if revisions != list(range(1, revisions[-1] + 1)):
            issues.append({'code': 'revision_gap', 'event': ident})
        if len({row['visit'] for row in values}) != 1:
            issues.append({'code': 'event_changes_visit', 'event': ident})
            continue
        times = [instant(row['observed_at'], 'observed_at') for row in ordered]
        if times != sorted(times) or len(times) != len(set(times)):
            issues.append({'code': 'nonincreasing_revision_observation', 'event': ident})
            continue
        known = [row for row in ordered if instant(row['observed_at'], 'observed_at') <= cutoff]
        if not known:
            omitted.append({'event': ident, 'reason': 'not_yet_observed'})
            continue
        current = known[-1]
        if current['void']:
            omitted.append({'event': ident, 'reason': 'voided', 'revision': current['revision']})
        else:
            selected.append(current)
    body = {'knowledge_at': knowledge_at, 'events': selected, 'omitted': omitted, 'issues': issues}
    return {**body, 'selection_digest': digest(body)}


def affected_visits(before, after):
    old = {row['id']: row for row in before['events']}
    new = {row['id']: row for row in after['events']}
    result = set()
    changes = []
    for ident in sorted(old.keys() | new.keys()):
        left, right = old.get(ident), new.get(ident)
        if left == right:
            continue
        for row in (left, right):
            if row:
                result.add(row['visit'])
        changes.append({'event': ident, 'before_revision': left['revision'] if left else None, 'after_revision': right['revision'] if right else None})
    return {'visits': sorted(result), 'changes': changes}
