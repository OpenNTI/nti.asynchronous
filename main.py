#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import time
import signal
import argparse

from nti.graphdb.async.reactor import JobReactor

from nti.dataserver.utils import run_with_dataserver

def main():
    arg_parser = argparse.ArgumentParser(description="Initialize a class with sample data")
    arg_parser.add_argument('-v', '--verbose', help="Be verbose", action='store_true',
                             dest='verbose')
    arg_parser.add_argument('env_dir', help="Dataserver environment root directory")
    args = arg_parser.parse_args()
    env_dir = args.env_dir

    conf_packages = ('nti.appserver', 'nti.graphdb')
    run_with_dataserver(environment_dir=env_dir,
                        xmlconfig_packages=conf_packages,
                        verbose=args.verbose,
                        function=lambda: _process_args(args))

def _process_args(args):
    reactor = JobReactor()

    curr_sigint_handler = signal.getsignal(signal.SIGINT)
    def sigint_handler(*args):
        reactor.halt()
        logger.info("Shutting down...")
        while reactor.processor is not None:
            time.sleep(1)
        curr_sigint_handler(*args)

    def handler(*args):
        raise SystemExit()

    signal.signal(signal.SIGINT, sigint_handler)
    signal.signal(signal.SIGTERM, handler)

    reactor(time.sleep)

if __name__ == '__main__':
    main()
