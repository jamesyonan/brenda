# Brenda -- Blender render tool for Amazon Web Services
# Copyright (C) 2013 James Yonan <james@openvpn.net>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import sys, os, re

class Config(dict):
    # support key=value settings
    #   KEY="value"
    re_key_value = re.compile(r"(\w+)=(.*)")

    # support standard shell-style macro expansion, such as:
    #   MYFILE="$HOME/file.txt"
    re_macro = re.compile(r"\$\{?(\w+)\}?")

    def __init__(self, config_file, env_prefix=None, default_stdin=False, use_s3cfg=True):
        # load and parse config file
        if config_file:
            with open(config_file) as f:
                for line in f.readlines():
                    self._process_line(line)
        elif default_stdin:
            for line in sys.stdin.readlines():
                self._process_line(line)

        # load environmental vars
        self._load_from_env(env_prefix)

        # get access_key and secret_key from ~/.s3cfg (it it exists)
        if use_s3cfg:
            for k, s3k in (('AWS_ACCESS_KEY', 'access_key'), ('AWS_SECRET_KEY', 'secret_key')):
                if not self.get(k):
                    v = self._s3cfg_get(s3k)
                    if v:
                        self[k] = v

    @staticmethod
    def _s3cfg_get(key):
        r = re.compile(r"^%s\s*=\s*(.*)$" % (key,))
        home = os.path.expanduser("~")
        s3cfg = os.path.join(home, ".s3cfg")
        with open(s3cfg) as f:
            for line in f.readlines():
                m = re.match(r, line)
                if m:
                    return m.groups()[0]

    def _load_from_env(self, env_prefix):
        if env_prefix:
            for k, v in os.environ.iteritems():
                if k.startswith(env_prefix):
                    self[k[len(env_prefix):]] = v

    def _process_line(self, line):
        line = line.strip()
        m = re.match(self.re_key_value, line)
        if m:
            k, v = m.groups()
            sq = False
            if v and v[0] in ("'", '"') and v[0] == v[-1]:
                sq = (v[0] == "'")
                v = v[1:-1]
            if not sq:
                v = re.sub(self.re_macro, self._repfn, v)
            self[k] = v

    def _repfn(self, m):
        k, = m.groups()
        if k in self:
            return self[k]
        elif k in os.environ:
            return os.environ[k]
        else:
            return ''
