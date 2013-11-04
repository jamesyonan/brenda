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

def makedirs(dir):
    print "MAKEDIRS", dir
    os.makedirs(dir)

def mv(src, dest):
    print "MV %s %s" % (src, dest)
    shutil.move(src, dest)

def shutdown():
    print "SHUTDOWN"
    system(["/sbin/shutdown", "-h", "0"])

def write_atomic(path, data):
    tmp = path + '.tmp'
    with open(tmp, 'w') as f:
        f.write(data)
    os.rename(tmp, path)

def str_nl(s):
    if len(s) > 0 and s[-1] != '\n':
        s += '\n'
    return s

def blkdev(index, istore=False, mount_form=False):
    if istore:
        # instance store
        devs = ('b', 'c', 'd', 'e')
    else:
        # EBS
        devs = (
            'f1', 'g1', 'h1', 'i1', 'j1', 'k1', 'l1', 'm1', 'n1', 'o1', 'p1',
            'f2', 'g2', 'h2', 'i2', 'j2', 'k2', 'l2', 'm2', 'n2', 'o2', 'p2',
            'f3', 'g3', 'h3', 'i3', 'j3', 'k3', 'l3', 'm3', 'n3', 'o3', 'p3',
            'f4', 'g4', 'h4', 'i4', 'j4', 'k4', 'l4', 'm4', 'n4', 'o4', 'p4',
            'f5', 'g5', 'h5', 'i5', 'j5', 'k5', 'l5', 'm5', 'n5', 'o5', 'p5',
            'f6', 'g6', 'h6', 'i6', 'j6', 'k6', 'l6', 'm6', 'n6', 'o6', 'p6',
            )
    if mount_form:
        return '/dev/xvd' + devs[index]
    else:
        return '/dev/sd' + devs[index]

def mount(dev, dir, mkfs=False):
    if not os.path.isdir(dir):
        mkdir(dir)
        if mkfs:
            system(["/sbin/mkfs", "-t", "ext4", dev])
        system(["/bin/mount", dev, dir])

def top_dir(dir):
    """
    If dir contains no files and only one directory,
    return that directory.  Otherwise return dir.
    Note file/dir ignore rules.
    """
    def ignore(fn):
        return fn == 'lost+found' or fn.startswith('.') or fn.endswith('.etag')
    for dirpath, dirnames, filenames in os.walk(dir):
        dirs = []
        for f in filenames:
            if not ignore(f):
                break
        else:
            for d in dirnames:
                if not ignore(d):
                    dirs.append(d)
        if len(dirs) == 1:
            return os.path.join(dirpath, dirs[0])
        else:
            return dirpath

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
    def g():
        if opt:
            return opt
        else:
            ret = conf.get(conf_key, default)
            if not ret and must_exist:
                raise ValueError("config key %r is missing" % (conf_key,))
            return ret
    ret = g()
    if ret == '*':
        if must_exist:
            raise ValueError("config key %r must not be wildcard" % (conf_key,))
        return None
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
