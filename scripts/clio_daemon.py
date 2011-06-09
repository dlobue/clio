
import logging

import simpledaemon

class clio_daemon(simpledaemon.Daemon):
    default_conf = 'clio_daemon.conf'
    section = 'clio'

    def run(self):
        import eventlet
        from clio.store import app
        logger = logging.getLogger()
        if logger.handlers:
            [app.logger.addHandler(h) for h in logger.handlers]
        app.logger.setLevel(logger.level)
        eventlet.serve(eventlet.listen((app.config['HOST'], app.config['PORT']), backlog=2048), app)

if __name__ == '__main__':
    clio_daemon().main()

