import boto
import paracurl

conn = boto.connect_s3()
buck = conn.get_bucket('marble_factory')
k = boto.s3.key.Key(buck)
k.key = 'marble-factory-lossless.mov'
url = k.generate_url(600, force_http=True)

try:
    status = paracurl.download('foo.dat', url, max_threads=64, debug=1)
except paracurl.Exception, e:
    print "Caught Exception:", e
else:
    print "Return Value:", status

