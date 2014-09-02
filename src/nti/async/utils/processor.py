#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from nti.monkey import relstorage_patch_all_except_gevent_on_import
relstorage_patch_all_except_gevent_on_import.patch()

import os
import sys
import time
import signal
import logging
import argparse

import zope.exceptions
import zope.browserpage

from zope import component
from zope.container.contained import Contained
from zope.configuration import xmlconfig, config
from zope.dottedname import resolve as dottedname

from z3c.autoinclude.zcml import includePluginsDirective

from nti.dataserver.interfaces import IRedisClient
from nti.dataserver.utils import run_with_dataserver
from nti.dataserver.interfaces import IDataserverTransactionRunner

from nti.contentlibrary.interfaces import IContentPackageLibrary

from ..reactor import AsyncReactor
from ..reactor import AsyncFailedReactor

from ..interfaces import IQueue
from ..interfaces import IRedisQueue
from ..redis_queue import RedisQueue

# signal handlers

def handler(*args):
	raise SystemExit()

def sigint_handler(*args):
	logger.info("Shutting down %s", os.getpid())
	sys.exit(0)

signal.signal(signal.SIGTERM, handler)
signal.signal(signal.SIGINT, sigint_handler)

# package loader info

class PluginPoint(Contained):

	def __init__(self, name):
		self.__name__ = name

PP_APP = PluginPoint('nti.app')
PP_APP_SITES = PluginPoint('nti.app.sites')
PP_APP_PRODUCTS = PluginPoint('nti.app.products')

class Processor(object):

	conf_package = 'nti.appserver'

	def create_arg_parser(self):
		arg_parser = argparse.ArgumentParser(description="Async processor")
		arg_parser.add_argument('-v', '--verbose', help="Be verbose",
								 action='store_true', dest='verbose')
		arg_parser.add_argument('-l', '--library', help="Load library packages",
								action='store_true', dest='library')
		arg_parser.add_argument('-n', '--name', help="Queue name", default='',
								dest='name')
		arg_parser.add_argument('-o', '--queue_names', help="Queue names", default='',
								dest='queue_names')
		arg_parser.add_argument('--no_exit', help="Whether to exit on errors",
								 default=True, dest='exit_error',action='store_false')
		arg_parser.add_argument('--site', dest='site', help="request SITE")
		arg_parser.add_argument('--redis', help="Use redis queues",
								 action='store_true', dest='redis')
		arg_parser.add_argument('--failed_jobs', help="Process failed jobs",
								 action='store_true', dest='failed_jobs')
		return arg_parser

	def create_context(self, env_dir):
		etc = os.getenv('DATASERVER_ETC_DIR') or os.path.join(env_dir, 'etc')
		etc = os.path.expanduser(etc)

		context = config.ConfigurationMachine()
		xmlconfig.registerCommonDirectives(context)

		slugs = os.path.join(etc, 'package-includes')
		if os.path.exists(slugs) and os.path.isdir(slugs):
			package = dottedname.resolve('nti.dataserver')
			context = xmlconfig.file('configure.zcml', package=package, context=context)
			xmlconfig.include(context, files=os.path.join(slugs, '*.zcml'),
							  package=self.conf_package)

		library_zcml = os.path.join(etc, 'library.zcml')
		if not os.path.exists(library_zcml):
			raise Exception("Could not locate library zcml file %s", library_zcml)

		xmlconfig.include(context, file=library_zcml, package=self.conf_package)

		# Include zope.browserpage.meta.zcm for tales:expressiontype
		# before including the products
		xmlconfig.include(context, file="meta.zcml", package=zope.browserpage)

		# include plugins
		includePluginsDirective(context, PP_APP)
		includePluginsDirective(context, PP_APP_SITES)
		includePluginsDirective(context, PP_APP_PRODUCTS)

		return context

	def set_log_formatter(self, args):
		ei = '%(asctime)s %(levelname)-5.5s [%(name)s][%(thread)d][%(threadName)s] %(message)s'
		logging.root.handlers[0].setFormatter(zope.exceptions.log.Formatter(ei))

	def setup_redis_queues(self, queue_names):
		redis = component.getUtility(IRedisClient)
		all_queues = list(queue_names)
		for name in all_queues:
			queue = RedisQueue(redis, name)
			component.globalSiteManager.registerUtility(queue, IRedisQueue, name)

	def _load_library(self):
		library = component.queryUtility(IContentPackageLibrary)
		library.syncContentPackages()

	def process_args(self, args):
		self.set_log_formatter(args)

		name = getattr(args, 'name', None) or u''
		queue_names = getattr(args, 'queue_names', None)

		if not name and not queue_names:
			raise ValueError('No queue name(s) passed in' )

		if name and not queue_names:
			queue_names = [name]

		if getattr(args, 'redis', False):
			queue_interface = IRedisQueue
			self.setup_redis_queues(queue_names)
		else:
			queue_interface = IQueue

		failed_jobs = getattr( args, 'failed_jobs', False)

		if getattr( args, 'library', False ):
			transaction_runner = component.getUtility(IDataserverTransactionRunner)
			transaction_runner(self._load_library)

		site_names = [getattr(args, 'site', None)]
		exit_on_error = getattr(args, 'exit_error', True)

		if failed_jobs:
			target = AsyncFailedReactor(queue_names=queue_names,
										site_names=site_names,
								  		queue_interface=queue_interface)
			result = target()
		else:
			target = AsyncReactor(queue_names=queue_names,
								  exitOnError=exit_on_error,
								  site_names=site_names,
								  queue_interface=queue_interface)
			result = target(time.sleep)
		sys.exit(result)

	def __call__(self, *args, **kwargs):
		arg_parser = self.create_arg_parser()
		args = arg_parser.parse_args()

		env_dir = os.getenv('DATASERVER_DIR')
		env_dir = os.path.expanduser(env_dir) if env_dir else env_dir
		if not env_dir or not os.path.exists(env_dir) and not os.path.isdir(env_dir):
			raise IOError("Invalid dataserver environment root directory")

		context = self.create_context(env_dir)
		conf_packages = (self.conf_package, 'nti.async')

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
