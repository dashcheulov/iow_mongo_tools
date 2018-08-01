#!/usr/bin/env python3
""" Main module """
__author__ = "Denis Ashcheulov"
__version__ = "0.0.1"
__status__ = "Planning"

import sys
import pymongo


def hello():
    return "Hello world!"


def main():
    """ Main entrypoint """
    print(hello())
    sys.exit(0)
