from collections import defaultdict
from dispatch_log.schema import instant


def chains(dataset, timelines):
    by_id = {row['visit']: row for row in timelines}
    children = defaultdict(list)
    for visit in dataset.visits.values():
        if visit['parent'] is not None:
            children[visit['parent']].append(visit['id'])
    groups = []
    issues = []
    for ident, visit in sorted(dataset.visits.items()):
        if visit['parent'] is not None:
            continue
        pending = [ident]
        members = []
        while pending:
            current = pending.pop()
            members.append(current)
            next_visits = sorted(children[current])
            if len(next_visits) > 1:
                issues.append({'code': 'branching_retry_chain', 'parent': current, 'children': next_visits})
            for child in next_visits:
                requested = instant(dataset.visits[child]['requested_at'], 'requested_at')
                parent = by_id[current]
                milestones = parent['milestones']
                terminal = milestones.get('cancelled', []) + milestones.get('completed', [])
                if parent['state'] != 'not_requested' and not any(timestamp <= requested for timestamp in terminal):
                    issues.append({'code': 'retry_before_parent_terminal', 'parent': current, 'child': child})
            pending.extend(reversed(next_visits))
        observed = [by_id[key] for key in members if by_id[key]['state'] != 'not_requested']
        completed = [row for row in observed if row['valid'] and row['state'] == 'completed']
        first_request = instant(visit['requested_at'], 'requested_at')
        first_completion = min((row['milestones']['completed'][0] for row in completed), default=None)
        groups.append({
            'root': ident,
            'client': visit['client'],
            'service': visit['service'],
            'members': members,
            'observed_attempts': len(observed),
            'valid_completed_attempts': len(completed),
            'eventually_completed': bool(completed),
            'multiple_completions': len(completed) > 1,
            'first_completion_seconds_from_original_request': first_completion - first_request if first_completion is not None else None,
            'invalid_attempts': sum(not row['valid'] for row in observed),
            'count_unit': 'one_original_need_with_explicit_retry_links',
        })
    return {'chains': groups, 'issues': issues, 'unlinked_repeat_visits_are_not_inferred_retries': True}


def retry_summary(chain_report):
    relevant = [row for row in chain_report['chains'] if row['observed_attempts']]
    denominator = len(relevant)
    completed = sum(row['eventually_completed'] for row in relevant)
    return {
        'original_needs': denominator,
        'completed_original_needs': completed,
        'eventual_completion_rate': None if not denominator else {'numerator': completed, 'denominator': denominator},
        'attempts': sum(row['observed_attempts'] for row in relevant),
        'retried_needs': sum(row['observed_attempts'] > 1 for row in relevant),
        'multiple_completion_needs': sum(row['multiple_completions'] for row in relevant),
        'chain_issue_count': len(chain_report['issues']),
    }
