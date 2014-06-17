#!/usr/bin/env python
# -*- coding: utf-8 -*
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

from zope import component
from zope.configuration import xmlconfig, config
from zope.dottedname import resolve as dottedname

from nti.contentlibrary import interfaces as lib_interfaces

from nti.dataserver.utils import run_with_dataserver

from nti.async import reactor

def sigint_handler(*args):
	logger.info("Shutting down %s", os.getpid())
	sys.exit(0)

def handler(*args):
	raise SystemExit()

signal.signal(signal.SIGINT, sigint_handler)
signal.signal(signal.SIGTERM, handler)

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
		return arg_parser

	def create_context(self, env_dir=None):
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

		return context

	def set_log_formatter(self, args):
		ei = '%(asctime)s %(levelname)-5.5s [%(name)s][%(thread)d][%(threadName)s] %(message)s'
		logging.root.handlers[0].setFormatter(zope.exceptions.log.Formatter(ei))

	def process_args(self, args):
		self.set_log_formatter(args)

		if args.library:
			library = component.queryUtility(lib_interfaces.IContentPackageLibrary)
			getattr(library, 'contentPackages')

		name = args.name or u''
		target = reactor.AsyncReactor(name=name)
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
							function=lambda: self.process_args(args))

def main():
	return Processor()()

if __name__ == '__main__':
	main()
