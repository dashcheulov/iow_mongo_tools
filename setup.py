from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='iow-mongo-tools',
    version='0.0.1',
    description='Various tools for maintenance mongo cluster',
    long_description=long_description,
    url='https://confluence.iponweb.net/display/OPS/iow-mongo-tools',
    author='Denis Ashcheulov',
    author_email='dashcheulov@iponweb.net',
    license='GPL-3.0+',
    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: IOW SysOps',
        'Intended Audience :: IOW PMs',
        'License :: OSI Approved :: GPL-3.0+',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
    ],
    keywords='mongo iow',
    packages=find_packages(),
    install_requires=['pymongo>=3.7.1'],
    entry_points={
        'console_scripts': [
            'dummy=iowmongotools:main',
        ],
    },
)