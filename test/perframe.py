# test brenda-node by generating small text files
# as stand-ins for real frames

import os, sys, time, optparse

parser = optparse.OptionParser()

parser.add_option("-o", "--out", dest="out",
                  help="output file")

parser.add_option("-p", "--pause", type="int", dest="pause", default=1,
                      help="Pause delay per 'frame', default=%default")

parser.add_option("-s", "--start", type="int", dest="start",
                      help="start frame")

parser.add_option("-e", "--end", type="int", dest="end",
                      help="end frame")

parser.add_option("-j", "--step", type="int", dest="step",
                      help="frame increment")

( opts, args ) = parser.parse_args()

# generate test tmpfile
fn = os.path.join(os.environ['TMP'], "info.tmp")
with open(fn, 'w') as f:
    f.write("start=%d end=%d step=%d pause=%d out=%s\n" % (opts.start, opts.end, opts.step, opts.pause, opts.out))

for i in xrange(opts.start, opts.end+1, opts.step):
    # generate fake frame
    fn = opts.out.replace("######", "%06d") % (i,) + '.txt'
    print fn
    with open(fn, 'w') as f:
        f.write("This is a test, frame #%d\n" % (i,))
    time.sleep(opts.pause)
