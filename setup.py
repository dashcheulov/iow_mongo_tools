from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))
with open(path.join(here, 'README.rst')) as f:
    long_description = f.read()

setup(
    name='iow-mongo-tools',
    version='0.7.0',
    description='Various tools for maintenance mongo cluster',
    long_description=long_description,
    url='https://confluence.iponweb.net/display/OPS/iow-mongo-tools',
    author='Denis Ashcheulov',
    author_email='dashcheulov@iponweb.net',
    license='GPL-3.0+',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: IOW SysOps',
        'Intended Audience :: IOW PMs',
        'License :: OSI Approved :: GPL-3.0+',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
    ],
    keywords='mongo iow',
    packages=find_packages(),
    install_requires=['pymongo>=3.5.1'],
    setup_requires=['pytest-runner'],
    tests_require=['pytest', 'pyaml', 'mongomock'],
    entry_points={
        'console_scripts': [
            'mongo_check=iowmongotools:MongoCheckerCli.entry',
            'mongo_set=iowmongotools:MongoSetCli.entry',
            'mongo_clone=iowmongotools:MongoCloneCli.entry',
            'mongo_upload=iowmongotools:MongoUploadCli.entry',
        ],
    },
)
