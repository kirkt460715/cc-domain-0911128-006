import hashlib
from pathlib import Path
import sqlite3
from dispatch_log.ledger import Ledger
from operations.materializer import Materializer
from operations.publication import Publisher


def audit(ledger):
    source = ledger.audit()
    snapshots = Materializer(ledger).audit()
    from dispatch_log.staging import Staging
    staging = Staging(ledger).audit()
    from quality.cases import Cases
    cases = Cases(Materializer(ledger)).audit()
    publications = Publisher(Materializer(ledger)).audit()
    if ledger.db.execute('PRAGMA foreign_key_check').fetchall():
        raise ValueError('foreign key integrity failure')
    return {'valid': True, 'ledger': source, 'snapshots': snapshots, 'publications': publications, 'staging': staging, 'cases': cases}


def backup(ledger, destination):
    evidence = audit(ledger)
    target = Path(destination)
    if target.exists() or target.is_symlink():
        raise FileExistsError(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.touch(exist_ok=False)
    try:
        output = sqlite3.connect(str(target))
        try:
            ledger.db.backup(output)
        finally:
            output.close()
        with Ledger(target) as restored:
            if audit(restored) != evidence:
                raise ValueError('backup semantic audit mismatch')
    except BaseException:
        target.unlink(missing_ok=True)
        raise
    return {'sha256': hashlib.sha256(target.read_bytes()).hexdigest(), 'audit': evidence}


def restore(source, destination, checksum):
    origin, target = Path(source), Path(destination)
    if origin.is_symlink() or not origin.is_file():
        raise ValueError('backup must be regular file')
    if hashlib.sha256(origin.read_bytes()).hexdigest() != checksum:
        raise ValueError('backup checksum mismatch')
    if target.exists() or target.is_symlink():
        raise FileExistsError(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.touch(exist_ok=False)
    try:
        connection = sqlite3.connect(origin.resolve().as_uri() + '?mode=ro', uri=True)
        output = sqlite3.connect(str(target))
        try:
            connection.backup(output)
        finally:
            connection.close()
            output.close()
        with Ledger(target) as restored:
            evidence = audit(restored)
    except BaseException:
        target.unlink(missing_ok=True)
        raise
    return {'restored': True, 'audit': evidence}


def replay_batches(ledger, destination):
    target = Path(destination)
    if target.exists() or target.is_symlink():
        raise FileExistsError(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.touch(exist_ok=False)
    try:
        with Ledger(target) as restored:
            for item in ledger.export_batches():
                actual = restored.ingest(item['receipt']['id'], item['request'])
                if actual != item['receipt']:
                    raise ValueError('batch receipt replay mismatch')
            if restored.document().fingerprint != ledger.document().fingerprint:
                raise ValueError('batch replay source mismatch')
            evidence = restored.audit()
    except BaseException:
        target.unlink(missing_ok=True)
        raise
    return {'restored_registry_and_event_ledger': True, 'cached_reports_and_publications_not_copied': True, 'audit': evidence}
