import codecs
from setuptools import setup, find_packages

entry_points = {
    'z3c.autoinclude.plugin': [
        'target = nti.dataserver',
    ],
}

TESTS_REQUIRE = [
    'fakeredis',
    'fudge',
    'nti.dataserver[test]',
    'nti.testing',
    'zope.dottedname',
    'zope.testrunner',
    'nti.fakestatsd'
]


def _read(fname):
    with codecs.open(fname, encoding='utf-8') as f:
        return f.read()


setup(
    name='nti.asynchronous',
    version=_read('version.txt').strip(),
    author='Jason Madden',
    author_email='jason@nextthought.com',
    description="NTI Async",
    long_description=(_read('README.rst') + '\n\n' + _read('CHANGES.rst')),
    license='Apache',
    keywords='Queue reactor',
    classifiers=[
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
    ],
    url="https://github.com/OpenNTI/nti.asynchronous",
    zip_safe=True,
    packages=find_packages('src'),
    package_dir={'': 'src'},
    include_package_data=True,
    namespace_packages=['nti'],
    tests_require=TESTS_REQUIRE,
    install_requires=[
        'setuptools',
	    'nti.coremetadata',
        'nti.property',
        'nti.site',
        'nti.schema',
        'nti.transactions',
        'nti.zodb',
        'BTrees',
        'persistent',
        'redis',
        'six',
        'transaction',
        'zc.queue',
        'ZODB',
        'zope.annotation',
        'zope.component',
        'zope.configuration',
        'zope.exceptions',
        'zope.interface',
        'zope.location',
        'zope.security',
    ],
    extras_require={
        'test': TESTS_REQUIRE,
        'docs': [
            'persistent',
            'BTrees',
            'Sphinx',
            'repoze.sphinx.autointerface',
            'sphinx_rtd_theme',
            'zope.interface',
        ]
    },
    entry_points=entry_points,
)
