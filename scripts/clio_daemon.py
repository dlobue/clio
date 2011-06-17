
import simpledaemon

class clio_daemon(simpledaemon.Daemon):
    default_conf = '/etc/clio_daemon.conf'
    section = 'clio'

    def run(self):
        import eventlet
        from eventlet import wsgi
        from clio.store import app
        wsgi.server(eventlet.listen((app.config['HOST'], app.config['PORT']), backlog=2048), app)

if __name__ == '__main__':
    clio_daemon().main()

