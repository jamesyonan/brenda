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
    user = utils.get_opt(opts.user, conf, 'AWS_USER', default='root')
    args = ['ssh', '-o', 'UserKnownHostsFile=/dev/null',
                   '-o', 'StrictHostKeyChecking=no',
                   '-o', 'LogLevel=quiet']
    if user:
        args.extend(['-o', 'User='+user])
    args.extend(['-i', aws.get_adaptive_ssh_identity_fn(opts, conf)])
    return args

def ssh_cmd_list(opts, conf, args, instances=None):
    if instances is None:
        instances = aws.filter_instances(opts, conf)
    for i in instances:
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

def perf(opts, conf, args):
    def task_count_last(i):
        s = i[1].split()
        try:
            count = int(s[0])
            last = int(s[1])
        except:
            return None
        else:
            return count, last

    script = ['if', '[', '-f', 'task_count', ']', '&&', '[', '-f', 'task_last', '];', 'then', 'cat', 'task_count;', 'cat', 'task_last;', 'else', 'echo', '0;', 'fi']
    instances = aws.filter_instances(opts, conf)
    idict = dict([(i.dns_name, i) for i in instances])
    sdict = aws.get_spot_request_dict(conf)
    data = {}
    for i in run_cmd_list(opts, conf, ssh_cmd_list(opts, conf, script, instances), show_output=False, capture_stderr=False):
        host = i[0]
        inst = idict.get(host)
        if inst:
            sir = sdict.get(inst.spot_instance_request_id)
            price = None
            if sir:
                price = float(sir.price)
            tasks = task_count_last(i)
            if tasks:
                task_count, task_last = tasks
                uptime = aws.get_uptime(task_last, inst.launch_time) / 3600.0
                stat = data.setdefault(inst.instance_type, dict(n=0, uptime_sum=0.0, task_sum=0, price_sum=0.0))
                stat['n'] += 1
                stat['uptime_sum'] += uptime
                stat['task_sum'] += task_count
                if price is not None:
                    stat['price_sum'] += price
    tph= []
    tpd = []
    total_tasks = 0.0
    total_uptime = 0
    total_n = 0
    for itype, stat in data.items():
        total_tasks += stat['task_sum']
        total_uptime += stat['uptime_sum']
        total_n += stat['n']
        tasks_per_hour = stat['task_sum'] / stat['uptime_sum']
        tph.append((tasks_per_hour, itype))
        if 'price_sum' in stat:
            mprice = stat['price_sum'] / stat['n']
            tasks_per_dollar = tasks_per_hour / mprice
            tpd.append((tasks_per_dollar, itype))
    tph.sort(reverse=True)
    tpd.sort(reverse=True)
    if total_n:
        print "Tasks per hour (%.02f)" % (total_tasks / total_uptime * total_n,)
        for tasks_per_hour, itype in tph:
            print "  %s %.02f" % (itype, tasks_per_hour)
        print "Tasks per US$"
        for tasks_per_dollar, itype in tpd:
            print "  %s %.02f" % (itype, tasks_per_dollar)
