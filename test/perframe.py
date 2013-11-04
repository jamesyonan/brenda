# test brenda-node by generating small text files
# as stand-ins for real frames

import os, sys, time, optparse

parser = optparse.OptionParser()

parser.add_option("-o", "--outdir", dest="outdir",
                  help="output directory")

parser.add_option("-p", "--pause", type="int", dest="pause", default=1,
                      help="Pause delay per 'frame', default=%default")

parser.add_option("-s", "--start", type="int", dest="start",
                      help="start frame")

parser.add_option("-e", "--end", type="int", dest="end",
                      help="end frame")

parser.add_option("-j", "--step", type="int", dest="step",
                      help="frame increment")

( opts, args ) = parser.parse_args()

for i in xrange(opts.start, opts.end+1, opts.step):
    fn = os.path.join(opts.outdir, "frame%04d.txt" % (i,))
    print fn
    with open(fn, 'w') as f:
        f.write("This is a test, frame #%d\n" % (i,))
    time.sleep(opts.pause)
