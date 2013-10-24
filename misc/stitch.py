# Stitch subframe tiles into a single frame.
# Very slow, intended as proof-of-concept only.

import sys
from PIL import Image

X = 1920
Y = 1080

out = Image.new('RGB', (X, Y), (0, 0, 0)) 

data = []

for png in sys.argv[1:]:
    im = Image.open(png)
    pix = im.load()
    data.append(pix)

for x in xrange(X):
    for y in xrange(Y):
        color = (0, 0, 0)
        for d in data:
            c = d[x, y]
            if c[0] or c[1] or c[2]:
                color = c
                break
        out.putpixel((x, y), color)

out.save("final.png", "PNG")
