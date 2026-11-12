import argparse
import json
from pathlib import Path
import sys
import tempfile
from dispatch_log.schema import Dataset, canonical
from dispatch_log.ledger import Ledger
from dispatch_log import tabular
from operations.pipeline import analyze, audit
from operations.materializer import Materializer
from operations.publication import Publisher
from operations.query import report
from operations import recovery, bundle
from operations.comparison import compare, impact_by_region


def load(path):
    source = Path(path)
    if source.is_symlink() or not source.is_file() or source.stat().st_size > 50_000_000:
        raise ValueError('input must be regular bounded JSON file')
    return json.loads(source.read_text(encoding='utf-8'))


def demo(path):
    dataset = Dataset(load(path))
    with tempfile.TemporaryDirectory(prefix='visits-demo-') as directory:
        root = Path(directory)
        with Ledger(root / 'visits.sqlite3') as ledger:
            ledger.import_document('initial', dataset.document, 0)
            materializer = Materializer(ledger)
            first = materializer.produce('daily-v1', '2026-11-10T00:00:00Z', '2026-11-12T00:00:00Z', '2026-11-12T00:00:00Z', '2026-11-12T12:00:00Z')
            second = materializer.produce('daily-v2', '2026-11-10T00:00:00Z', '2026-11-12T00:00:00Z', '2026-11-12T00:00:00Z', '2026-11-12T12:00:00Z')
            if second['cache']['recomputed_visits'] != 0 or first['result'] != second['result']:
                raise ValueError('contribution cache reuse failed')
            Publisher(materializer).publish('public-v1', 'daily-v1')
            exported = bundle.write(materializer.dataset('daily-v1'), first['result'], root / 'bundle')
            tabular.write_directory(dataset, root / 'tables')
            if tabular.read_directory(root / 'tables').fingerprint != dataset.fingerprint:
                raise ValueError('CSV roundtrip mismatch')
            saved = recovery.backup(ledger, root / 'backup.sqlite3')
            restored = recovery.restore(root / 'backup.sqlite3', root / 'restored.sqlite3', saved['sha256'])
            replayed = recovery.replay_batches(ledger, root / 'replayed.sqlite3')
            return {
                'sla': first['result']['sla_overall'],
                'quality_counts': first['result']['quality']['counts'],
                'cache_reuse': second['cache'],
                'bundle': exported,
                'backup_restore': restored,
                'batch_replay': replayed,
                'source': 'local_synthetic_fixture',
                'runtime_downloads': False,
            }


def parser():
    root = argparse.ArgumentParser(description='Home visit fulfillment and worktime analytics')
    commands = root.add_subparsers(dest='command', required=True)
    demonstration = commands.add_parser('demo')
    demonstration.add_argument('--fixture', required=True)
    ingest = commands.add_parser('ingest')
    ingest.add_argument('--database', required=True)
    ingest.add_argument('--id', required=True)
    ingest.add_argument('--input', required=True)
    ingest.add_argument('--generation', type=int)
    ingest.add_argument('--full', action='store_true')
    analyze = commands.add_parser('analyze')
    analyze.add_argument('--input', required=True)
    produce = commands.add_parser('produce')
    produce.add_argument('--database', required=True)
    produce.add_argument('--id', required=True)
    produce.add_argument('--generation', type=int)
    for command in (analyze, produce):
        command.add_argument('--start', required=True)
        command.add_argument('--end', required=True)
        command.add_argument('--effective-at', required=True)
        command.add_argument('--knowledge-at', required=True)
        command.add_argument('--offset-minutes', type=int, default=0)
    reporting = commands.add_parser('report')
    reporting.add_argument('--database', required=True)
    reporting.add_argument('--id', required=True)
    reporting.add_argument('--kind', required=True)
    reporting.add_argument('--options')
    comparison = commands.add_parser('compare')
    comparison.add_argument('--database', required=True)
    comparison.add_argument('--before', required=True)
    comparison.add_argument('--after', required=True)
    publishing = commands.add_parser('publish')
    publishing.add_argument('--database', required=True)
    publishing.add_argument('--id', required=True)
    publishing.add_argument('--label', required=True)
    publishing.add_argument('--dimension', choices=['region', 'service'], default='region')
    publishing.add_argument('--minimum-clients', type=int, default=3)
    export = commands.add_parser('export')
    export.add_argument('--database', required=True)
    export.add_argument('--id', required=True)
    export.add_argument('--directory', required=True)
    verify = commands.add_parser('verify-bundle')
    verify.add_argument('--directory', required=True)
    for name in ('csv-in', 'csv-out'):
        command = commands.add_parser(name)
        command.add_argument('--input', required=True)
        command.add_argument('--output', required=True)
    auditing = commands.add_parser('audit')
    auditing.add_argument('--database', required=True)
    for name in ('backup', 'replay-batches'):
        command = commands.add_parser(name)
        command.add_argument('--database', required=True)
        command.add_argument('--output', required=True)
    restore = commands.add_parser('restore')
    restore.add_argument('--input', required=True)
    restore.add_argument('--output', required=True)
    restore.add_argument('--sha256', required=True)
    api = commands.add_parser('serve')
    api.add_argument('--database', required=True)
    api.add_argument('--token-file', required=True)
    api.add_argument('--port', type=int, default=8080)
    stage = commands.add_parser('stage')
    stage.add_argument('--database', required=True)
    stage.add_argument('--operation', choices=['begin', 'part', 'promote', 'status'], required=True)
    stage.add_argument('--body', required=True)
    cases = commands.add_parser('case')
    cases.add_argument('--database', required=True)
    cases.add_argument('--operation', choices=['open', 'resolve', 'get', 'pending'], required=True)
    cases.add_argument('--body', required=True)
    return root


def execute(args):
    if args.command == 'serve':
        from operations.http_api import serve
        path = Path(args.token_file)
        if path.is_symlink() or path.stat().st_mode & 0o077:
            raise ValueError('token file must be owner-only regular file')
        serve(args.database, path.read_text().strip(), args.port)
        return {'stopped': True}
    if args.command == 'demo':
        return demo(args.fixture)
    if args.command == 'analyze':
        return analyze(Dataset(load(args.input)), args.start, args.end, args.effective_at, args.knowledge_at, args.offset_minutes)
    if args.command == 'csv-out':
        return tabular.write_directory(Dataset(load(args.input)), args.output)
    if args.command == 'csv-in':
        dataset = tabular.read_directory(args.input)
        with Path(args.output).open('x', encoding='utf-8') as output:
            output.write(canonical(dataset.document))
        return {'input_digest': dataset.fingerprint}
    if args.command == 'restore':
        return recovery.restore(args.input, args.output, args.sha256)
    if args.command == 'verify-bundle':
        return bundle.verify(args.directory)
    with Ledger(args.database) as ledger:
        if args.command == 'stage':
            from dispatch_log.staging import Staging
            return staging_action(Staging(ledger), args.operation, load(args.body))
        if args.command == 'case':
            from quality.cases import Cases
            return case_action(Cases(Materializer(ledger)), args.operation, load(args.body))
        if args.command == 'ingest':
            if args.full:
                return ledger.import_document(args.id, load(args.input), args.generation)
            return ledger.ingest(args.id, load(args.input), args.generation)
        if args.command == 'audit':
            return recovery.audit(ledger)
        if args.command == 'backup':
            return recovery.backup(ledger, args.output)
        if args.command == 'replay-batches':
            return recovery.replay_batches(ledger, args.output)
        materializer = Materializer(ledger)
        if args.command == 'produce':
            return materializer.produce(args.id, args.start, args.end, args.effective_at, args.knowledge_at, args.offset_minutes, args.generation)
        if args.command == 'report':
            result = materializer.report(args.id)['result']
            return report(materializer.dataset(args.id), result, args.kind, load(args.options) if args.options else {})
        if args.command == 'publish':
            return Publisher(materializer).publish(args.label, args.id, args.dimension, args.minimum_clients)
        if args.command == 'compare':
            before = materializer.report(args.before)['result']
            after = materializer.report(args.after)['result']
            return {'visits_and_workers': compare(before, after), 'regions': impact_by_region(before, after)}
        if args.command == 'export':
            return bundle.write(materializer.dataset(args.id), materializer.report(args.id)['result'], args.directory)
    raise ValueError('unsupported command')


def staging_action(stage, operation, body):
    from dispatch_log.schema import fields
    if operation == 'begin':
        fields(body, ('id', 'parts', 'expected_digest'))
        return stage.begin(body['id'], body['parts'], body['expected_digest'])
    if operation == 'part':
        fields(body, ('id', 'index', 'body'))
        return stage.part(body['id'], body['index'], body['body'])
    if operation == 'promote':
        fields(body, ('id', 'batch'), ('generation',))
        return stage.promote(body['id'], body['batch'], body.get('generation'))
    fields(body, ('id',))
    return stage.status(body['id'])


def case_action(cases, operation, body):
    from dispatch_log.schema import fields
    if operation == 'open':
        fields(body, ('id', 'report', 'finding_digest', 'actor'))
        return cases.open(body['id'], body['report'], body['finding_digest'], body['actor'])
    if operation == 'resolve':
        fields(body, ('id', 'revision', 'evidence_report', 'actor', 'reason'))
        return cases.resolve(body['id'], body['revision'], body['evidence_report'], body['actor'], body['reason'])
    if operation == 'get':
        fields(body, ('id',))
        return cases.get(body['id'])
    fields(body, ())
    return cases.pending()


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        result = execute(args)
    except (ValueError, KeyError, OSError) as exc:
        print(canonical({'error': str(exc)}), file=sys.stderr)
        return 2
    print(canonical(result))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
