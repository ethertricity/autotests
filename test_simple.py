__author__ = 'andrew'

import sys
import json

args = sys.argv

resultfile = args[1]
n = int(args[2])

m = n + n

results = {
    "outputname": "Ok",
    "fields": {"m": str(m)}
}

result_ = open(resultfile, "w")
result_.write(json.dumps(results))
result_.close()