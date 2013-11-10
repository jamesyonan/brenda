from distutils.core import setup, Extension

# original version
VERSION="0.5"

# get current version
with open('brenda/version.py') as f:
    exec(f.read())

paracurl_ext = Extension("paracurl", ["paracurl/paracurl.c"],
                         libraries=['curl'])

setup(name = "Brenda",
      version = VERSION,
      packages = [ 'brenda' ],
      scripts = [ 'brenda-work', 'brenda-tool', 'brenda-run', 'brenda-node', 'brenda-ebs' ],
      ext_modules = [paracurl_ext],

      data_files=[('brenda/task-scripts', ['task-scripts/frame', 'task-scripts/subframe']),
                  ('brenda/doc', ['README', 'doc/brenda-talk-blendercon-2013.pdf'])],

      author = "James Yonan",
      author_email = "james@openvpn.net",
      description = "Blender render farm tool for Amazon Web Services",
)
