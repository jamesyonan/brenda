from distutils.core import setup, Extension
import platform

# original version
VERSION="0.5"

# get current version
with open('brenda/version.py') as f:
    exec(f.read())

ext_modules = []

if platform.system() == 'Linux':
    ext_modules = [ Extension("paracurl", ["paracurl/paracurl.c"],
                              libraries=['curl']) ]

setup(name = "Brenda",
      version = VERSION,
      packages = [ 'brenda' ],
      scripts = [ 'brenda-work', 'brenda-tool', 'brenda-run', 'brenda-node', 'brenda-ebs' ],
      ext_modules = ext_modules,

      data_files=[('brenda/task-scripts', ['task-scripts/frame', 'task-scripts/subframe']),
                  ('brenda/doc', ['README.md', 'doc/brenda-talk-blendercon-2013.pdf'])],

      author = "James Yonan",
      author_email = "james@openvpn.net",
      description = "Blender render farm tool for Amazon Web Services",
)
