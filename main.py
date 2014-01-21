#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import os
import sys
import time
import signal
import argparse

from nti.graphdb.async.reactor import JobReactor

from nti.dataserver.utils import run_with_dataserver

def sigint_handler(*args):
    logger.info("Shutting down %s", os.getpid())
    sys.exit(0)

def handler(*args):
    raise SystemExit()

signal.signal(signal.SIGINT, sigint_handler)
signal.signal(signal.SIGTERM, handler)

def main():
    arg_parser = argparse.ArgumentParser(description="Graphdb job processor")
    arg_parser.add_argument('-v', '--verbose', help="Be verbose", action='store_true',
                             dest='verbose')
    arg_parser.add_argument('env_dir', help="Dataserver environment root directory")
    args = arg_parser.parse_args()
    env_dir = args.env_dir

    conf_packages = ('nti.appserver', 'nti.graphdb')
    run_with_dataserver(environment_dir=env_dir,
                        xmlconfig_packages=conf_packages,
                        verbose=args.verbose,
                        minimal_ds=False,
                        function=lambda: _process_args(args))

def _tone_down_logging():
    import logging
    try:
        package = 'py2neo.packages.httpstream.http'
        __import__(package)
        py2neo_logger = logging.getLogger(package)
        py2neo_logger.setLevel(logging.ERROR)
    except ImportError:
        logger.error("could not setup logging level for py2neo")

def _process_args(args):
    if not args.verbose:
        _tone_down_logging()
    reactor = JobReactor()
    reactor(time.sleep)

if __name__ == '__main__':
    main()
