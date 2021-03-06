import os

from werkzeug.wrappers import Request, Response

from werkzeug import cached_property
from werkzeug.routing import Map
from werkzeug.routing import Rule
from werkzeug.wsgi import ClosingIterator
from werkzeug.wsgi import SharedDataMiddleware

from handlers import welcome
from handlers import search
from handlers import collections
from handlers import bookmarks
from handlers import category_name_filter

#v2
from handlers import collections_v2
from handlers import category_name_filter_v2
from handlers import events

from middleware import ErrorMiddleware

STATIC_PATH = os.path.join(os.path.dirname(__file__), 'static')
#STATIC_PATH = os.path.join('/Users/mafaq/Dropbox/', 'SnugabugPhotos')
STATIC_PATH = '/data/'

class SnugRequest(Request):
    def __init__(self, *args, **kw):
        Request.__init__(self, *args, **kw)


class SnugabugApp(object):

    def __init__(self):
        pass

    def __call__(self, environ, start_response):
        return self.wsgi_app(environ, start_response)

    def wsgi_app(self, environ, start_response):
        urls = self.url_map.bind_to_environ(environ)
        endpoint, args = urls.match()
        request = SnugRequest(environ)
        response = endpoint(request, **args)
        return ClosingIterator(response(environ, start_response), [])

    @cached_property
    def url_map(self):
        return make_url_map()

def make_url_map():
    url_map = Map([
        Rule('/',
            endpoint=welcome,
            strict_slashes=False),

        Rule('/v1/search',
            endpoint=search,
            strict_slashes=False),

        Rule('/v1/collections',
            endpoint=collections,
            strict_slashes=False),

        Rule('/v1/bookmarks',
            endpoint=bookmarks,
            strict_slashes=False),

        Rule('/v1/category_name_filter',
            endpoint=category_name_filter,
            strict_slashes=False),

        #V2
        Rule('/v2/search',
            endpoint=search,
            strict_slashes=False),

        Rule('/v2/collections',
            endpoint=collections_v2,
            strict_slashes=False),

        Rule('/v2/bookmarks',
            endpoint=bookmarks,
            strict_slashes=False),

        Rule('/v2/category_name_filter',
            endpoint=category_name_filter_v2,
            strict_slashes=False),

        Rule('/v2/events',
            endpoint=events,
            strict_slashes=False),

        Rule('/favicon.ico', endpoint=lambda request: Response(status=404)),
        Rule('/error.gif', endpoint=lambda request: Response(status=404)),
        ])
    return url_map


def make_app(debug=False, with_static=True):
    app = SnugabugApp()

    if with_static:
        app.wsgi_app = SharedDataMiddleware(
            app.wsgi_app, {
                '/images':  STATIC_PATH,
            }
            )

    if debug:
        from werkzeug.debug import DebuggedApplication
        from wsgiref.validate import validator
        app = DebuggedApplication(app, evalex=True)
        app = validator(app)

    return app


def runserver(ip, port, debug=False):  # pragma: no cover
    from cherrypy import wsgiserver
    from werkzeug.serving import run_with_reloader

    app = ErrorMiddleware(make_app(debug=debug))
    server = wsgiserver.CherryPyWSGIServer((ip, port), app, numthreads=30)
    run_with_reloader(server.start)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Sungabug v1.0')
    parser.add_argument('ip', nargs='?', default='0.0.0.0',
                        help="defaults to %(default)s")
    parser.add_argument('-p', '--port', type=int, default=80,
                        help="defaults to %(default)s")
    parser.add_argument('-d', '--debug', action='store_true', default=False,
                        help="show traceback interpreter on error")
    args = parser.parse_args()
    runserver(args.ip, args.port, args.debug)
else:
    application = ErrorMiddleware(make_app())
