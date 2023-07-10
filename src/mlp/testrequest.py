import urllib.request
import ssl
from io import StringIO
import sys

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

req = urllib.request.urlopen(sys.argv[1], context=ctx)
res = req.read()
print(StringIO(res.decode('utf-8')).getvalue())