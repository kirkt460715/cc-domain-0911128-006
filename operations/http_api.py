from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlsplit
import hmac
import json
from dispatch_log.schema import fields, canonical
from dispatch_log.ledger import Ledger
from operations.materializer import Materializer
from operations.publication import Publisher
from operations.query import report
from operations.comparison import compare


def dispatch(ledger, operation, body):
    materializer = Materializer(ledger)
    if operation == 'report':
        fields(body, ('id', 'kind'), ('options',))
        result = materializer.report(body['id'])['result']
        return report(materializer.dataset(body['id']), result, body['kind'], body.get('options', {}))
    if operation == 'snapshot':
        fields(body, ('id',))
        return materializer.report(body['id'])
    if operation == 'publication':
        fields(body, ('label',))
        return Publisher(materializer).get(body['label'])
    if operation == 'batch':
        fields(body, ('id',))
        return ledger.batch(body['id'])
    if operation == 'compare':
        fields(body, ('before', 'after'))
        return compare(materializer.report(body['before'])['result'], materializer.report(body['after'])['result'])
    if operation == 'status':
        fields(body, ())
        return {
            'generation': ledger.generation(),
            'reports': [row[0] for row in ledger.db.execute('SELECT id FROM reports ORDER BY id')],
            'batches': [row[0] for row in ledger.db.execute('SELECT id FROM batches ORDER BY sequence')],
        }
    raise ValueError('unknown read operation')


def handler(database, token):
    if not isinstance(token, str) or len(token) < 24:
        raise ValueError('API token must have at least 24 characters')

    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def log_message(self, *args):
            pass

        def reply(self, status, body):
            content = canonical(body).encode()
            self.send_response(status)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Content-Length', str(len(content)))
            self.send_header('Cache-Control', 'no-store')
            self.send_header('Connection', 'close')
            self.end_headers()
            self.wfile.write(content)
            self.close_connection = True

        def do_GET(self):
            self.reply(405, {'error': 'authenticated_POST_required'})

        def do_POST(self):
            if not hmac.compare_digest(self.headers.get('Authorization', ''), 'Bearer ' + token):
                self.reply(401, {'error': 'unauthorized'})
                return
            lengths = self.headers.get_all('Content-Length', [])
            if self.headers.get('Transfer-Encoding') is not None or len(lengths) != 1 or not lengths[0].isascii() or not lengths[0].isdigit():
                self.reply(400, {'error': 'single_content_length_required'})
                return
            length = int(lengths[0])
            if length > 1000000:
                self.reply(413, {'error': 'request_too_large'})
                return
            if self.headers.get_content_type() != 'application/json':
                self.reply(415, {'error': 'JSON_required'})
                return
            path = urlsplit(self.path)
            if path.query or path.fragment or not path.path.startswith('/read/'):
                self.reply(404, {'error': 'unknown_route'})
                return
            operation = path.path[6:]
            if not operation or '/' in operation:
                self.reply(404, {'error': 'unknown_route'})
                return
            self.connection.settimeout(10)
            try:
                raw = self.rfile.read(length)
                if len(raw) != length:
                    raise ValueError('incomplete request')
                body = json.loads(raw)
                with Ledger(database) as ledger:
                    result = dispatch(ledger, operation, body)
            except KeyError:
                self.reply(404, {'error': 'not_found'})
            except (ValueError, TypeError, UnicodeError) as exc:
                self.reply(400, {'error': str(exc)[:1000]})
            except TimeoutError:
                self.reply(408, {'error': 'request_timeout'})
            else:
                self.reply(200, {'result': result})

    return Handler


def serve(database, token, port=8080):
    if type(port) is not int or not 0 <= port <= 65535:
        raise ValueError('invalid port')
    with HTTPServer(('127.0.0.1', port), handler(database, token)) as server:
        server.serve_forever(poll_interval=.2)
