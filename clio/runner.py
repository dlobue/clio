
import logging
from logging.handlers import RotatingFileHandler
from threading import Timer

import eventlet
from eventlet import wsgi
from clio.store import app

def _rotate_logs(file_handler, rotate_interval):
    try:
        return file_handler.doRollover()
    finally:
        rotate_timer = Timer(rotate_interval, _rotate_logs, (file_handler, rotate_interval))
        rotate_timer.start()

def main():
    file_handler = RotatingFileHandler()
    file_handler.setLevel(getattr(logging, app.config.get('LOG_LEVEL', 'INFO').upper()))
    app.logger.addHandler(file_handler)
    rotate_interval = int(app.config.get('LOG_ROTATE_INTERVAL', 1440)) * 60
    rotate_timer = Timer(rotate_interval, _rotate_logs, (file_handler, rotate_interval))
    rotate_timer.start()

    wsgi.server(eventlet.listen((app.config['HOST'], app.config['PORT']), backlog=2048), app)


