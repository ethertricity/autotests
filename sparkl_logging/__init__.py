"""
Copyright 2016 Sparkl Limited. All Rights Reserved.
Authors: Andrew Farrell <ahfarrell@sparkl.com>
Library for handling logging.
"""
import logging
import os


def getlogger(name):
    return logging.getLogger(name)


def setlogger(filename, name, path=None):
    if path is not None:
        filename = os.path.join(path, filename)

    logging.basicConfig(
        filename=filename,
        filemode='w',
        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s' +
               '%(message)s line:%(lineno)d',
        datefmt='%H:%M:%S',
        level=logging.DEBUG)
    return getlogger(name)
