from setuptools import setup

setup(
    name='pyhs2',
    version='0.6.1',
    author='RadiumOne Data Team',
    author_email='dwteam@RadiumOne.com',
    packages=['pyhs2', 'pyhs2/cloudera', 'pyhs2/TCLIService'],
    url='http://git.dw.sc.gwallet.com:7990/projects/DATA/repos/pyhs2/browse',
    license='LICENSE.txt',
    description='Python Hive Server 2 Client Driver',
    long_description=open('README.md').read(),
    install_requires=["sasl","thrift","dtepy"],
    dependency_links=['http://dt01.etl.dw.sc.gwallet.com:7001/packages/'],
    test_suite='pyhs2.test'
)
