import csv
import io
import json
from pathlib import Path
from dispatch_log.schema import Dataset, canonical


SCHEMA = {
    'services': [('id', 'text'), ('response_seconds', 'int'), ('minimum_active_seconds', 'int'), ('maximum_active_seconds', 'int')],
    'workers': [('id', 'text'), ('region', 'text'), ('daily_regular_seconds', 'int')],
    'visits': [('id', 'text'), ('client', 'text'), ('service', 'text'), ('region', 'text'), ('requested_at', 'text'), ('promised_start', 'text'), ('parent', 'nullable')],
    'shifts': [('id', 'text'), ('worker', 'text'), ('start', 'text'), ('end', 'text'), ('breaks', 'json')],
    'events': [('id', 'text'), ('revision', 'int'), ('visit', 'text'), ('sequence', 'int'), ('effective_at', 'text'), ('observed_at', 'text'), ('kind', 'text'), ('worker', 'nullable'), ('reason', 'nullable'), ('void', 'bool')],
}


def parse(value, kind):
    if kind == 'text':
        return value
    if kind == 'nullable':
        return value or None
    if kind == 'int':
        if not value or not value.isascii() or not value.isdigit():
            raise ValueError('integer CSV cells must be unsigned ASCII digits')
        return int(value)
    if kind == 'bool':
        if value not in ('true', 'false'):
            raise ValueError('boolean CSV cells must be true or false')
        return value == 'true'
    if kind == 'json':
        return json.loads(value)
    raise ValueError('unknown cell type')


def read_table(table, text):
    if table not in SCHEMA or not isinstance(text, str) or len(text.encode()) > 50_000_000:
        raise ValueError('unknown table or excessive CSV size')
    schema = dict(SCHEMA[table])
    try:
        reader = csv.reader(io.StringIO(text), strict=True)
        header = next(reader, None)
        if not header or len(header) != len(set(header)) or set(header) != set(schema):
            raise ValueError('CSV header mismatch')
        rows = []
        for cells in reader:
            if len(cells) != len(header):
                raise ValueError('CSV row width mismatch')
            rows.append({key: parse(value, schema[key]) for key, value in zip(header, cells)})
            if len(rows) > 100000:
                raise ValueError('CSV exceeds record limit')
        return rows
    except csv.Error as exc:
        raise ValueError('malformed CSV') from exc


def write_table(dataset, table):
    if table not in SCHEMA:
        raise ValueError('unknown table')
    output = io.StringIO(newline='')
    writer = csv.writer(output, lineterminator='\n')
    writer.writerow([key for key, kind in SCHEMA[table]])
    for row in dataset.document[table]:
        cells = []
        for key, kind in SCHEMA[table]:
            value = row[key]
            if value is None:
                value = ''
            elif kind == 'bool':
                value = 'true' if value else 'false'
            elif kind == 'json':
                value = canonical(value)
            cells.append(value)
        writer.writerow(cells)
    return output.getvalue()


def read_directory(directory):
    root = Path(directory)
    if root.is_symlink() or not root.is_dir():
        raise ValueError('table directory must be regular')
    if {path.name for path in root.iterdir()} != {name + '.csv' for name in SCHEMA}:
        raise ValueError('CSV file set mismatch')
    data = {'version': 1}
    for name in SCHEMA:
        path = root / (name + '.csv')
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 50_000_000:
            raise ValueError('CSV must be bounded regular file')
        data[name] = read_table(name, path.read_text(encoding='utf-8-sig'))
    return Dataset(data)


def write_directory(dataset, directory):
    root = Path(directory)
    if root.exists() or root.is_symlink():
        raise FileExistsError(root)
    root.mkdir(parents=True)
    written = []
    try:
        for name in SCHEMA:
            path = root / (name + '.csv')
            path.write_text(write_table(dataset, name), encoding='utf-8')
            written.append(path)
    except BaseException:
        for path in written:
            path.unlink()
        root.rmdir()
        raise
    return {'input_digest': dataset.fingerprint, 'files': sorted(path.name for path in written)}
