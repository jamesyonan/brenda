import boto
import paracurl

conn = boto.connect_s3()
buck = conn.get_bucket('marble_factory')
k = boto.s3.key.Key(buck)
k.key = 'blend.gz'
url = k.generate_url(600, force_http=True)

try:
    etag = '779ccf330e7c227ebbf7b34f0a6bae79foo'
    status = paracurl.download('foo.dat', url, max_threads=4, n_retries=4, etag=etag, debug=2)
except paracurl.Exception, e:
    print "Caught Exception:", e
else:
    print "Return Value:", status
