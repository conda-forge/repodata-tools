import base64
import sys


with open(sys.argv[1], "r") as fp:
    pem = fp.read()

print(base64.b64encode(pem.encode()).decode())
