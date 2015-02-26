import codecs
from setuptools import setup, find_packages

VERSION = '0.0.0'

entry_points = {
	'z3c.autoinclude.plugin': [
		'target = nti.dataserver',
	],
}

setup(
	name='nti.async',
	version=VERSION,
	author='Jason Madden',
	author_email='jason@nextthought.com',
	description="NTI Async",
	long_description=codecs.open('README.rst', encoding='utf-8').read(),
	license='Proprietary',
	keywords='Queue Reactor',
	classifiers=[
		'Intended Audience :: Developers',
		'Natural Language :: English',
		'Operating System :: OS Independent',
		'Programming Language :: Python :: 2',
		'Programming Language :: Python :: 2.7',
		'Programming Language :: Python :: Implementation :: CPython'
	],
	packages=find_packages('src'),
	package_dir={'': 'src'},
	namespace_packages=['nti'],
	install_requires=[
		'setuptools',
		'zope.exceptions',
		'nti.schema'
	],
	entry_points=entry_points
)
