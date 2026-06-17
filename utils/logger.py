import logging
import logging.handlers
from concurrent_log_handler import ConcurrentRotatingFileHandler
import socket
import os
import sys

def setup_log(
        app: str,
        environment: str = None,
        minimum_level: int = logging.INFO, 
        filename: str = None, 
        backup_count: int = 10,
        alert_to: str = None,
        alert_minimum_level: int = logging.ERROR,
        rolling_max_bytes: int = 10*1024*1024,
        use_concurrent_file_handler = True,
        use_stream=False
    ):

    environment = environment or _get_environment()
    filename = filename or f'{app}.log'
    dirname = os.path.dirname(filename)
    if dirname:
        os.makedirs(dirname, exist_ok=True)

    log_format = "%(asctime)s [%(threadName)-12.12s] [%(levelname)-8.8s] [%(filename)s:%(lineno)d] %(message)s"
    log_formatter = logging.Formatter(log_format)
    logging.basicConfig(
        level=minimum_level,
        format=log_format,
        handlers=[]
    )

    root = logging.getLogger()


    if use_stream:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(log_formatter)
        root.addHandler(stream_handler)

    concurrent_rotating_filehandler = ConcurrentRotatingFileHandler(filename=filename, backupCount=backup_count, maxBytes=rolling_max_bytes)
    concurrent_rotating_filehandler.setFormatter(log_formatter)

    rotating_filehandler = logging.handlers.RotatingFileHandler(filename=filename, backupCount=backup_count, maxBytes=rolling_max_bytes)
    rotating_filehandler.setFormatter(log_formatter)
    
    root.addHandler(concurrent_rotating_filehandler if use_concurrent_file_handler else rotating_filehandler)

    if alert_to:
        root.addHandler(_get_smtp_handler(app, environment, append_freepoint_domain(alert_to), alert_minimum_level))


def append_freepoint_domain(comma_separated_emails: str):
    emails = [email.strip() for email in comma_separated_emails.split(',')]
    return ','.join([email if '@' in email else f'{email}@freepoint.com' for email in emails]) 


def _get_smtp_handler(app: str, environment:str, alert_to: str, alert_minimum_level: str):   
    smtp_handler = logging.handlers.SMTPHandler(
        mailhost=('smtp.freepoint.local', 0),
        fromaddr='noreply@freepoint.com',
        toaddrs=[email.strip() for email in alert_to.split(',')],
        subject=f'[{environment}] Log Alert: {app}'
    )
    smtp_handler.setLevel(alert_minimum_level)
    return smtp_handler


def _get_environment() -> str:
    hostname = socket.gethostname().casefold()
    if hostname.startswith('prd'):
        return 'Production'
    elif hostname.startswith('tst'):
        return 'Test'
    else:
        return 'Development'