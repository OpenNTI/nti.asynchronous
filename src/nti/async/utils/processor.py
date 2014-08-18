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

from nti.contentlibrary.interfaces import IContentPackageLibrary

from nti.dataserver.utils import run_with_dataserver

from nti.async.reactor import AsyncReactor

def handler(*args):
	raise SystemExit()

def sigint_handler(*args):
	logger.info("Shutting down %s", os.getpid())
	sys.exit(0)

signal.signal(signal.SIGTERM, handler)
signal.signal(signal.SIGINT, sigint_handler)

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

	def setup_site(self, args):
		site = getattr(args, 'site', None)
		if site:
			from pyramid.testing import DummyRequest
			from pyramid.testing import setUp as psetUp

			request = DummyRequest()
			config = psetUp(registry=component.getGlobalSiteManager(),
							request=request,
							hook_zca=False)
			config.setup_registry()
			request.headers['origin'] = \
							'http://' + site if not site.startswith('http') else site
			# zope_site_tween tweaks some things on the request that we need to as well
			request.possible_site_names = \
					(site if not site.startswith('http') else site[7:],)

	def process_args(self, args):
		self.setup_site(args)
		self.set_log_formatter(args)

		if getattr(args, 'library', False):
			library = component.queryUtility(IContentPackageLibrary)
			getattr(library, 'contentPackages', None)

		name = getattr(args, 'name', None) or u''
		queue_names = getattr(args, 'queue_names', None)

		if name is None or queue_names is None:
			raise ValueError( 'No queue_name(s) passed in' )

		if name is not None and queue_names is None:
			queue_names = [name]

		exit_on_error = getattr(args, 'exit_error', True)
		target = AsyncReactor(queue_names=queue_names, exitOnError=exit_on_error)
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
