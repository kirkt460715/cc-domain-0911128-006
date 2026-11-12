import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
from dispatch_log.schema import Dataset, canonical
from operations.pipeline import audit
from operations.publication import safe_sla, verify as verify_public
from quality.latency import event_latency, revision_churn, lateness_buckets
from quality.waits import waiting_observations, survival


def contents(dataset, result, minimum_clients=3):
    checked = audit(dataset, result, replay_all=True)
    packet = safe_sla(result, minimum_clients=minimum_clients)
    verify_public(packet)
    documents = {
        'input.json': dataset.document,
        'result.json': result,
        'audit.json': checked,
        'public.json': packet,
        'event-latency.json': event_latency(dataset, result['knowledge_at']),
        'revision-churn.json': revision_churn(dataset, result['knowledge_at']),
        'latency-buckets.json': lateness_buckets(dataset, result['knowledge_at']),
        'wait-to-complete.json': survival(waiting_observations(dataset, result['timeline']['visits'], result['effective_at'], 'completed')),
    }
    files = {name: canonical(value) for name, value in documents.items()}
    manifest = {
        'version': 1,
        'input_digest': dataset.fingerprint,
        'result_digest': result['result_digest'],
        'minimum_clients': minimum_clients,
        'files': {name: hashlib.sha256(value.encode()).hexdigest() for name, value in sorted(files.items())},
        'access': 'restricted_contains_synthetic_or_authorized_client_and_worker_data',
    }
    files['manifest.json'] = canonical(manifest)
    return files


def write(dataset, result, directory, minimum_clients=3):
    files = contents(dataset, result, minimum_clients)
    target = Path(directory)
    if target.exists() or target.is_symlink():
        raise FileExistsError(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix='.visits-', dir=target.parent))
    try:
        for name, content in files.items():
            with (staging / name).open('w', encoding='utf-8') as output:
                output.write(content)
                output.flush()
                os.fsync(output.fileno())
        os.rename(staging, target)
    except BaseException:
        shutil.rmtree(staging)
        raise
    return verify(target)


def verify(directory):
    root = Path(directory)
    if root.is_symlink() or not root.is_dir():
        raise ValueError('bundle must be regular directory')
    source = root / 'manifest.json'
    if source.is_symlink() or not source.is_file() or source.stat().st_size > 100000:
        raise ValueError('invalid manifest')
    manifest = json.loads(source.read_text(encoding='utf-8'))
    if manifest.get('version') != 1 or not isinstance(manifest.get('files'), dict):
        raise ValueError('unsupported manifest')
    if {path.name for path in root.iterdir()} != set(manifest['files']) | {'manifest.json'}:
        raise ValueError('bundle file set mismatch')
    for name, expected in manifest['files'].items():
        if Path(name).name != name or name in ('.', '..'):
            raise ValueError('unsafe bundle path')
        path = root / name
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 100_000_000:
            raise ValueError('bundle files must be regular and bounded')
        if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise ValueError('bundle checksum mismatch')
    dataset = Dataset(json.loads((root / 'input.json').read_text(encoding='utf-8')))
    result = json.loads((root / 'result.json').read_text(encoding='utf-8'))
    expected = contents(dataset, result, manifest['minimum_clients'])
    for name, value in expected.items():
        if (root / name).read_bytes() != value.encode():
            raise ValueError('bundle derived content replay mismatch')
    return {'verified': True, 'files': len(expected), 'result_digest': result['result_digest']}
