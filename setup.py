from distutils.core import setup, Extension

paracurl_ext = Extension("paracurl", ["paracurl/paracurl.c"],
                         libraries=['curl'])

setup(name = "Brenda",
      version = "0.5",
      packages = [ 'brenda' ],
      scripts = [ 'brenda-work', 'brenda-tool', 'brenda-run', 'brenda-node' ],
      ext_modules = [paracurl_ext],

      author = "James Yonan",
      author_email = "james@openvpn.net",
      description = "Blender render farm tool for Amazon Web Services",
)
