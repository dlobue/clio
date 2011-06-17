
import simpledaemon

class runner(simpledaemon.Daemon):
    default_conf = '/etc/clio/daemon.conf'
    section = 'clio'

    def run(self):
        import eventlet
        from eventlet import wsgi
        from clio.store import app
        wsgi.server(eventlet.listen((app.config['HOST'], app.config['PORT']), backlog=2048), app)

if __name__ == '__main__':
    runner().main()

