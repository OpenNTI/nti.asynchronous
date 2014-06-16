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

def main():
	arg_parser = argparse.ArgumentParser(description="Async processor")
	arg_parser.add_argument('-v', '--verbose', help="Be verbose", action='store_true',
							 dest='verbose')
	arg_parser.add_argument('-l', '--library', help="Load library packages", action='store_true',
							 dest='library')
	arg_parser.add_argument('-n', '--name', help="Queue name", default='', dest='name')

	args = arg_parser.parse_args()
	env_dir = os.getenv('DATASERVER_DIR')
	if not env_dir or not os.path.exists(env_dir) and not os.path.isdir(env_dir):
		raise IOError("Invalid dataserver environment root directory")

	context = _create_context(env_dir)
	conf_packages = ('nti.appserver', 'nti.async')
	run_with_dataserver(environment_dir=env_dir,
						xmlconfig_packages=conf_packages,
						verbose=args.verbose,
						context=context,
						minimal_ds=True,
						function=lambda: _process_args(args))

def _create_context(env_dir):
	env_dir = os.path.expanduser(env_dir)
	etc = os.getenv('DATASERVER_ETC_DIR') or os.path.join(env_dir, 'etc')
	etc = os.path.expanduser(etc)

	context = config.ConfigurationMachine()
	xmlconfig.registerCommonDirectives(context)

	slugs = os.path.join(etc, 'package-includes')
	if os.path.exists(slugs) and os.path.isdir(slugs):
		package = dottedname.resolve('nti.dataserver')
		context = xmlconfig.file('configure.zcml', package=package, context=context)
		xmlconfig.include(context, files=os.path.join(slugs, '*.zcml'),
						  package='nti.appserver')

	library_zcml = os.path.join(etc, 'library.zcml')
	if not os.path.exists(library_zcml):
		raise Exception("Could not locate library zcml file %s", library_zcml)
		
	xmlconfig.include(context, file=library_zcml, package='nti.appserver')
		
	return context

def _process_args(args):
	ei = '%(asctime)s %(levelname)-5.5s [%(name)s][%(thread)d][%(threadName)s] %(message)s'
	logging.root.handlers[0].setFormatter(zope.exceptions.log.Formatter(ei))

	if args.library:
		library = component.queryUtility(lib_interfaces.IContentPackageLibrary)
		getattr(library, 'contentPackages')

	name = args.name or u''
	target = reactor.AsyncReactor(name=name)
	result = target(time.sleep)
	sys.exit(result)

if __name__ == '__main__':
	main()
