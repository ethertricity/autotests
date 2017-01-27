#!/usr/bin/python
__author__ = 'andrew'

import logging
import json
import os
import sys
from time import sleep

logging.basicConfig(
    filename='/tmp/container-info.log',
    filemode='a',
    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s line:%(lineno)d',
    datefmt='%H:%M:%S',
    level=logging.DEBUG)

logger = logging.getLogger(__name__)

from sparkl_services import (
    ws,
    s_op_rr,
    register_eventcallback
)


def handle_request(opname, fields):
    logger.debug(opname)
    logger.debug(str(fields))
    if opname == "FirstDivisor":
        return handle_fd()
    elif opname == "Test":
        return handle_test(fields)
    elif opname == "Iterate":
        return handle_iterate(fields)


def handle_fd():
    return True, 'Divisor', {'div': 2}


def handle_iterate(fields):
    n = int(fields.get('n'))
    div = int(fields.get('div'))

    if div == 2:
        res = 3
    else:
        res = div+2

    return True, 'Iterate', {'div': res, 'n': n}


def handle_test(fields):
    n = int(fields.get('n'))
    div = int(fields.get('div'))
    square = div*div
    rem = n % div

    if square > n:
        output = "Yes"
        field = "YES"
    elif rem == 0:
        output = "No"
        field = "NO"
    else:
        output = "Maybe"
        field = "MAYBE"

    return True, output, {field: field}

if __name__ == '__main__':

    args = sys.argv

    logger.debug(args)

    host = args[1]
    port = str(args[2])
    processid = args[3]
    url_stub = args[4]
    secure = True if args[5] == 'true' else False
    encoding = 'json'

    url_ws = \
        host+":"+port+"/"+url_stub+"_ws/connect?pending="+processid+"&encoding="+encoding

    logger.debug(url_ws)

    logger.debug("register for callback on requests")
    register_eventcallback(s_op_rr, handle_request)

    logger.debug("create and open the websocket for comms")
    ws.start(url_ws, args)
