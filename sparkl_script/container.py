#!/usr/bin/python
"""
Copyright 2016 Sparkl Limited. All Rights Reserved.
Authors: Andrew Farrell <ahfarrell@sparkl.com>
Implements a minimal script service to run inside a container.
"""

import sys
import os
from datetime import datetime

modulepath = os.path.abspath(__file__)
ext_root = \
    os.path.join(
        modulepath.split('/priv')[0],
        '..')

ext_root_dirs = \
    [d for d in
     [os.path.join(ext_root, d, 'priv', 'scripts') for d in os.listdir(ext_root)]
     if os.path.isdir(d)]
sys.path.extend(ext_root_dirs)

from sparkl_logging import setlogger
from sparkl_services import (
    ws
)

now = str((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds())

if __name__ == '__main__':

    args = sys.argv

    logger = setlogger("/tmp/container_debug_"+now+".log", __name__)

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

    logger.debug("create and open the websocket for comms")
    ws.start(url_ws, args)
