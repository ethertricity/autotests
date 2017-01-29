"""
Copyright 2016 Sparkl Limited. All Rights Reserved.
Authors: Andrew Farrell <ahfarrell@sparkl.com>
Module for services to handle communication with SPARKL, where protocol is
websockets.
A service register itself with handlers for
particular events and then proceeds to start the service.
"""

import logging
import websocket
import json
import sparkl_services
import os

envpath = None
logger = logging.getLogger(__name__)


def start(hosturl, args):
    """
    Init a websocket communication, and run over it forever

    :type: str
    :param hosturl: the url to connect with

    :type: str
    :param args: original container args
    """

    # make an "environment" directory for the principal service instance id

    global envpath

    logger.debug(args)

    secure = False
    envpath = os.getcwd()

    expected = 7
    if len(args) == expected:
        secure = True if args[5] == 'true' else False
        envpath = args[6]

    logger.debug(secure)
    logger.debug(envpath)

    websocket.enableTrace(True)
    wsprefix = "ws"
    if secure:
        wsprefix += "s"
    ws = websocket.WebSocketApp(
        wsprefix+'://'+hosturl,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close)
    ws.on_open = on_open
    sparkl_services.handle = \
        lambda event: sendevent(ws, json.dumps(event))
    ws.run_forever()


def sendevent(ws, event):
    logger.debug("sending event: "+str(event))
    ws.send(event)


def on_error(_, error):
    logger.error(error)


def on_close(_):
    logger.info("### closed websocket ")


def on_open(_):
    logger.info("### open websocket")


def on_message(ws, message):
    """
    Handles messages received on websocket
    :param ws: websocket handle
    :param message: received message
    """

    # call transport neutral handling library
    reply = sparkl_services.handle_msg(message, envpath)

    logger.debug(str(reply))

    # send reply back over ws transport
    if reply is not None:
        sendevent(ws, json.dumps(reply))
