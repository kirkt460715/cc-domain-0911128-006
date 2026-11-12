from collections import defaultdict
from dispatch_log.schema import instant, integer


def distribution(values):
    ordered = sorted(values)
    if not ordered:
        return {'count': 0, 'minimum': None, 'maximum': None, 'median': None, 'p90': None, 'sum': 0}
    def percentile(numerator, denominator):
        rank = (numerator * len(ordered) + denominator - 1) // denominator
        return ordered[max(0, rank - 1)]
    return {'count': len(ordered), 'minimum': ordered[0], 'maximum': ordered[-1], 'median': percentile(1, 2), 'p90': percentile(9, 10), 'sum': sum(ordered)}


def event_latency(dataset, knowledge_at):
    cutoff = instant(knowledge_at, 'knowledge_at')
    groups = defaultdict(list)
    for event in dataset.events:
        observed = instant(event['observed_at'], 'observed_at')
        if observed > cutoff:
            continue
        latency = observed - instant(event['effective_at'], 'effective_at')
        groups[(event['kind'], event['revision'] > 1)].append(latency)
    return {
        'knowledge_at': knowledge_at,
        'rows': [{'kind': kind, 'correction': correction, 'latency_seconds': distribution(values)} for (kind, correction), values in sorted(groups.items())],
        'observation_unit': 'every_known_revision_including_void_records',
    }


def revision_churn(dataset, knowledge_at):
    cutoff = instant(knowledge_at, 'knowledge_at')
    groups = defaultdict(list)
    for row in dataset.events:
        if instant(row['observed_at'], 'observed_at') <= cutoff:
            groups[row['id']].append(row)
    output = []
    for ident, versions in sorted(groups.items()):
        ordered = sorted(versions, key=lambda row: row['revision'])
        changed_fields = set()
        for left, right in zip(ordered, ordered[1:]):
            changed_fields.update(key for key in left if left[key] != right[key] and key not in ('revision', 'observed_at'))
        output.append({
            'event': ident,
            'visit': ordered[0]['visit'],
            'known_versions': len(ordered),
            'latest_revision': ordered[-1]['revision'],
            'currently_void': ordered[-1]['void'],
            'changed_fields': sorted(changed_fields),
            'seconds_between_first_and_last_observation': instant(ordered[-1]['observed_at'], 'observed_at') - instant(ordered[0]['observed_at'], 'observed_at'),
        })
    return output


def lateness_buckets(dataset, knowledge_at, thresholds=None):
    thresholds = [60, 3600, 86400] if thresholds is None else thresholds
    if not isinstance(thresholds, list) or thresholds != sorted(set(thresholds)):
        raise ValueError('latency bucket thresholds must increase strictly')
    for value in thresholds:
        integer(value, 'threshold', 0, 10**9)
    cutoff = instant(knowledge_at, 'knowledge_at')
    counts = [0] * (len(thresholds) + 1)
    for event in dataset.events:
        observed = instant(event['observed_at'], 'observed_at')
        if observed > cutoff:
            continue
        latency = observed - instant(event['effective_at'], 'effective_at')
        index = next((index for index, threshold in enumerate(thresholds) if latency <= threshold), len(thresholds))
        counts[index] += 1
    return [{'minimum_exclusive': None if index == 0 else thresholds[index - 1], 'maximum_inclusive': thresholds[index] if index < len(thresholds) else None, 'known_revisions': count} for index, count in enumerate(counts)]
