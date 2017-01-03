#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import os
import sys
import time
import signal
import logging
import argparse

import zope.exceptions

from zope import component

from nti.async.interfaces import IQueue
from nti.async.interfaces import IRedisQueue
from nti.async.interfaces import IAsyncReactor

from nti.async.reactor import AsyncReactor
from nti.async.reactor import AsyncFailedReactor

from nti.async.redis_queue import RedisQueue

from nti.dataserver.interfaces import IRedisClient
from nti.dataserver.interfaces import IDataserverTransactionRunner

from nti.dataserver.utils import run_with_dataserver
from nti.dataserver.utils.base_script import create_context

# signal handlers


def handler(*args):
    raise SystemExit()


def sigint_handler(*args):
    logger.info("Shutting down %s", os.getpid())
    sys.exit(0)

signal.signal(signal.SIGTERM, handler)
signal.signal(signal.SIGINT, sigint_handler)


class Processor(object):

    conf_package = 'nti.appserver'

    processor_name = "Async processor"

    def add_arg_parser_arguments(self, arg_parser):
        arg_parser.add_argument('-v', '--verbose', help="Be verbose",
                                action='store_true', dest='verbose')
        arg_parser.add_argument('-l', '--library', help="Load library packages",
                                                        action='store_true', dest='library')
        arg_parser.add_argument('-n', '--name', help="Queue name",
                                default=u'', dest='name')
        arg_parser.add_argument('-o', '--queue_names', help="Queue names", default='',
                                dest='queue_names')
        arg_parser.add_argument('--no_exit', help="Whether to exit on errors",
                                default=True, dest='exit_error', action='store_false')
        arg_parser.add_argument('--site', dest='site', help="Application SITE")
        arg_parser.add_argument('--redis', help="Use redis queues",
                                action='store_true', dest='redis')
        arg_parser.add_argument('--failed_jobs', help="Process failed jobs",
                                action='store_true', dest='failed_jobs')
        return arg_parser

    def create_arg_parser(self):
        arg_parser = argparse.ArgumentParser(description=self.processor_name)
        return self.add_arg_parser_arguments(arg_parser)

    def set_log_formatter(self, args):
        ei = '%(asctime)s %(levelname)-5.5s [%(name)s][%(thread)d][%(threadName)s] %(message)s'
        logging.root.handlers[0].setFormatter(
            zope.exceptions.log.Formatter(ei))

    def setup_redis_queues(self, queue_names):
        redis = component.getUtility(IRedisClient)
        all_queues = list(queue_names)
        for name in all_queues:
            queue = RedisQueue(redis, name)
            component.globalSiteManager.registerUtility(
                queue, IRedisQueue, name)

    def load_library(self):
        try:
            from nti.contentlibrary.interfaces import IContentPackageLibrary
            library = component.queryUtility(IContentPackageLibrary)
            if library is not None:
                library.syncContentPackages()
        except ImportError:
            logger.warn("Library not available")

    def process_args(self, args):
        self.set_log_formatter(args)

        name = getattr(args, 'name', None) or u''
        queue_names = getattr(args, 'queue_names', None)

        if not name and not queue_names:
            raise ValueError('No queue name(s) passed in')

        if name and not queue_names:
            queue_names = [name]

        if getattr(args, 'redis', False):
            queue_interface = IRedisQueue
            logger.info("Using redis queues")
            self.setup_redis_queues(queue_names)
        else:
            queue_interface = IQueue

        failed_jobs = getattr(args, 'failed_jobs', False)

        if getattr(args, 'library', False):
            transaction_runner = component.getUtility(
                IDataserverTransactionRunner)
            transaction_runner(self.load_library)
            logger.info("Library loaded")

        site = getattr(args, 'site', None)
        if site:
            logger.info("Using site %s", site)
        site_names = (site,) if site else ()

        exit_on_error = getattr(args, 'exit_error', True)

        if failed_jobs:
            target = AsyncFailedReactor(queue_names=queue_names,
                                        site_names=site_names,
                                        queue_interface=queue_interface)
            component.globalSiteManager.registerUtility(target, IAsyncReactor)
            result = target()
        else:
            target = AsyncReactor(queue_names=queue_names,
                                  exitOnError=exit_on_error,
                                  site_names=site_names,
                                  queue_interface=queue_interface)
            component.globalSiteManager.registerUtility(target, IAsyncReactor)
            result = target(time.sleep)
        sys.exit(result)

    def extend_context(self, context):
        pass

    def create_context(self, env_dir):
        context = create_context(env_dir, with_library=True)
        self.extend_context(context)
        return context

    def conf_packages(self):
        return (self.conf_package, 'nti.async')

    def __call__(self, *args, **kwargs):
        arg_parser = self.create_arg_parser()
        args = arg_parser.parse_args()

        env_dir = os.getenv('DATASERVER_DIR')
        env_dir = os.path.expanduser(env_dir) if env_dir else env_dir
        if not env_dir or not os.path.exists(env_dir) and not os.path.isdir(env_dir):
            raise IOError("Invalid dataserver environment root directory")

        context = self.create_context(env_dir)
        conf_packages = self.conf_packages()

        run_with_dataserver(environment_dir=env_dir,
                            xmlconfig_packages=conf_packages,
                            verbose=args.verbose,
                            context=context,
                            minimal_ds=True,
                            use_transaction_runner=False,
                            function=lambda: self.process_args(args))


def main():
    return Processor()()

if __name__ == '__main__':
    main()
