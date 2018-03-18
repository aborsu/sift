""" Logging Configuration """

import logging

def setup():
    fmt = '%(asctime)s|%(levelname)s|%(module)s|%(message)s'
    logging.basicConfig(format=fmt)
    log = logging.getLogger('nel')
    log.setLevel(logging.DEBUG)

def getLogger():
    return logging.getLogger('nel')

setup()