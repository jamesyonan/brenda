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

import os, subprocess, shutil

def system(cmd, ignore_errors=False):
    print "***", cmd
    succeed = 0
    ret = subprocess.call(cmd)
    if not ignore_errors and ret != succeed:
        raise ValueError("command failed with status %r (expected %r)" % (ret, succeed))

def rmtree(dir):
    print "RMTREE", dir
    shutil.rmtree(dir, ignore_errors=True)

def rm(file):
    print "RM", file
    try:
        os.remove(file)
    except:
        pass

def mkdir(dir):
    print "MKDIR", dir
    os.mkdir(dir)

def mv(src, dest):
    print "MV %s %s" % (src, dest)
    shutil.move(src, dest)

def shutdown():
    print "SHUTDOWN"
    system(["shutdown", "-h", "0"])

def write_atomic(path, data):
    tmp = path + '.tmp'
    with open(tmp, 'w') as f:
        f.write(data)
    os.rename(tmp, path)

def str_nl(s):
    if len(s) > 0 and s[-1] != '\n':
        s += '\n'
    return s

def system_return_output(cmd, capture_stderr=False):
    output = ""
    error = ""
    try:
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError, e:
        if capture_stderr:
            error = e.output
    return str_nl(output) + str_nl(error)

def get_opt(opt, conf, conf_key, default=None, must_exist=False):
    if opt:
        return opt
    else:
        ret = conf.get(conf_key, default)
        if not ret and must_exist:
            raise ValueError("config key %r is missing" % (conf_key,))
        return ret

class Cd(object):
    """
    Cd is a context manager that allows
    you to temporary change the working directory.

    with Cd(dir) as cd:
        ...
    """

    def __init__(self, directory):
        self._dir = directory

    def orig(self):
        return self._orig

    def dir(self):
        return self._dir

    def __enter__(self):
        self._orig = os.getcwd()
        os.chdir(self._dir)
        return self

    def __exit__(self, *args):
        os.chdir(self._orig)
