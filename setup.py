import codecs
from setuptools import setup, find_packages

VERSION = '0.0.0'

entry_points = {
    'z3c.autoinclude.plugin': [
        'target = nti.dataserver',
    ],
}

TESTS_REQUIRE = [
    'nose',
    'nose-timer',
    'nose-pudb',
    'nose-progressive',
    'nose2[coverage_plugin]',
    'pyhamcrest',
    'zope.testing',
    'nti.nose_traceback_info',
    'nti.testing'
]

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
        'BTrees',
        'gevent',
        'persistent',
        'transaction',
        'zc.blist',
        'zc.queue',
        'ZODB',
        'zope.annotation',
        'zope.component',
        'zope.exceptions',
        'zope.interface',
        'zope.location',
        'zope.security',
        'nti.property',
        'nti.site',
        'nti.schema',
        'nti.transactions',
        'nti.zodb'
    ],
    extras_require={
        'test': TESTS_REQUIRE,
    },
    entry_points=entry_points
)
