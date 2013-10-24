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

import threading, time, Queue
from brenda import aws, utils

def instances(opts, conf):
    now = time.time()
    for i in aws.filter_instances(opts, conf):
        uptime = aws.get_uptime(now, i.launch_time)
        print i.image_id, aws.format_uptime(uptime), i.public_dns_name

def ssh_args(opts, conf):
    user = utils.get_opt(opts.user, conf, 'AWS_USER')
    args = ['ssh', '-o', 'UserKnownHostsFile=/dev/null',
                   '-o', 'StrictHostKeyChecking=no',
                   '-o', 'LogLevel=quiet']
    if user:
        args.extend(['-o', 'User='+user])
    args.extend(['-i', aws.get_ssh_identity_fn(opts, conf)])
    return args

def ssh_cmd_list(opts, conf, args, hostset=None):
    for i in aws.filter_instances(opts, conf, hostset=hostset):
        node = i.public_dns_name
        cmd = ssh_args(opts, conf)
        cmd.append(node)
        cmd.extend(args)
        yield node, cmd

def rsync_cmd_list(opts, conf, args, hostset=None):
    for i in aws.filter_instances(opts, conf, hostset=hostset):
        node = i.public_dns_name
        cmd = ['rsync', '-e', ' '.join(ssh_args(opts, conf))] + [a.replace('HOST', node) for a in args]
        yield node, cmd

def run_cmd_list(opts, conf, cmd_seq, show_output, capture_stderr):
    def worker():
        while True:
            try:
                item = q.get(block=False)
            except Queue.Empty, e:
                break
            else:
                node, cmd = item
                output = utils.system_return_output(cmd, capture_stderr=capture_stderr)
                data = (node, output)
                with lock:
                    if show_output:
                        print "------- %s\n%s" % data,
                    ret.append(data)
                q.task_done()

    ret = []
    q = Queue.Queue()
    for task in cmd_seq:
        #if opts.verbose:
        #    print task
        q.put(task)

    lock = threading.Lock()
    max_threads = int(conf.get('TOOL_THREADS', '64'))
    for i in range(min(max_threads, q.qsize())):
        t = threading.Thread(target=worker)
        t.start()

    q.join() # block until all tasks are done
    return ret

def ssh(opts, conf, args):
    run_cmd_list(opts, conf, ssh_cmd_list(opts, conf, args), show_output=True, capture_stderr=True)

def rsync(opts, conf, args):
    run_cmd_list(opts, conf, rsync_cmd_list(opts, conf, args), show_output=True, capture_stderr=True)

def prune(opts, conf, args):
    def keyfunc(i):
        v = -1
        s = i[1].strip()
        if s == 'SMALL':
            v = -1
        elif s == 'BIG':
            v = 1<<32
        try:
            v = int(s)
        except:
            pass
        return v

    pidfile = conf.get('REMOTE_PIDFILE', 'brenda.pid')

    try:
        prune_target = int(args[0])
    except:
        raise ValueError("need prune target argument")
    else:
        if prune_target < 0:
            raise ValueError("prune target must be >= 0")

    if prune_target >= 0:
        # bash script logic to determine sort order for prune based on presence/absense
        # of files render.pid and task_last:
        #   if render.pid && task_last : return task_last
        #   if !render.pid && task_last : return BIG
        #   if render.pid && !task_last : return SMALL
        #   if !render.pid && !task_last : return SMALL
        script = ['if', '!', '[', '-f', 'task_last', '];', 'then', 'echo', 'SMALL;', 'elif', '[', '-f', pidfile, '];', 'then', 'cat', 'task_last;', 'else', 'echo', 'BIG;', 'fi']
        data = [(keyfunc(i), i[0]) for i in run_cmd_list(opts, conf, ssh_cmd_list(opts, conf, script), show_output=False, capture_stderr=False)]
        data.sort(reverse=True)
        print "Prune ranking data"
        for d in data:
            print d
        n_shutdown = len(data) - prune_target
        if n_shutdown > 0:
            shutdown_list = [i[1] for i in data[:n_shutdown]]
            print "Shutdown list"
            for sd in shutdown_list:
                print sd
            if not opts.dry_run:
                aws.shutdown_by_public_dns_name(opts, conf, shutdown_list)
