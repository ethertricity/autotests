__author__ = 'andrew'

import sys
import json

args = sys.argv

resultfile = args[1]

results = {
    "result": False
}

result_ = open(resultfile, "w")
result_.write(json.dumps(results))
result_.close()
