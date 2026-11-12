from collections import defaultdict
from fractions import Fraction
from dispatch_log.schema import instant


def waiting_observations(dataset, timelines, effective_at, target='started'):
    if target not in ('accepted', 'started', 'completed'):
        raise ValueError('unsupported waiting target')
    cutoff = instant(effective_at, 'effective_at')
    rows = []
    excluded = []
    for timeline in timelines:
        visit = dataset.visits[timeline['visit']]
        requested = instant(visit['requested_at'], 'requested_at')
        if requested > cutoff:
            continue
        if not timeline['valid']:
            excluded.append({'visit': visit['id'], 'reason': 'invalid_timeline'})
            continue
        milestones = timeline['milestones']
        event = milestones.get(target, [None])[0]
        cancelled = milestones.get('cancelled', [None])[0]
        if event is not None:
            duration = event - requested
            observed = True
        elif cancelled is not None:
            excluded.append({'visit': visit['id'], 'reason': 'cancelled_before_target', 'cancelled_at': cancelled})
            continue
        else:
            duration = cutoff - requested
            observed = False
        rows.append({'visit': visit['id'], 'region': visit['region'], 'service': visit['service'], 'seconds': duration, 'event_observed': observed})
    return {'target': target, 'rows': rows, 'excluded': excluded, 'cancellations_not_treated_as_independent_censoring': True}


def survival(wait_report):
    groups = defaultdict(lambda: {'events': 0, 'censors': 0})
    for row in wait_report['rows']:
        groups[row['seconds']]['events' if row['event_observed'] else 'censors'] += 1
    at_risk = len(wait_report['rows'])
    probability = Fraction(1)
    rows = []
    median = None
    for seconds, counts in sorted(groups.items()):
        before = at_risk
        if counts['events'] > before:
            raise ValueError('events exceed risk set')
        probability *= Fraction(before - counts['events'], before)
        if median is None and probability <= Fraction(1, 2):
            median = seconds
        rows.append({
            'seconds': seconds,
            'at_risk': before,
            'events': counts['events'],
            'censored': counts['censors'],
            'survival': {'numerator': probability.numerator, 'denominator': probability.denominator},
        })
        at_risk -= counts['events'] + counts['censors']
    return {
        'target': wait_report['target'],
        'observations': len(wait_report['rows']),
        'excluded_cancelled_or_invalid': len(wait_report['excluded']),
        'rows': rows,
        'median_seconds': median,
        'tie_order': 'events_before_censors_at_same_duration',
        'scope': 'descriptive_wait_among_non_cancelled_or_target_observed_visits_not_causal_competing_risk_estimate',
    }


def restricted_mean(wait_report, horizon_seconds):
    if type(horizon_seconds) is not int or horizon_seconds <= 0:
        raise ValueError('positive integer horizon required')
    estimate = survival(wait_report)
    if not estimate['observations']:
        return {'horizon_seconds': horizon_seconds, 'value': None, 'supported': False}
    maximum = max(row['seconds'] for row in wait_report['rows'])
    if horizon_seconds > maximum:
        raise ValueError('horizon exceeds observed followup support')
    probability = Fraction(1)
    last = 0
    integral = Fraction(0)
    for row in estimate['rows']:
        endpoint = min(row['seconds'], horizon_seconds)
        integral += (endpoint - last) * probability
        last = endpoint
        if endpoint == horizon_seconds:
            break
        probability = Fraction(row['survival']['numerator'], row['survival']['denominator'])
    return {'horizon_seconds': horizon_seconds, 'value': {'numerator': integral.numerator, 'denominator': integral.denominator}, 'supported': True}
