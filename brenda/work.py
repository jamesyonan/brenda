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

import random
from brenda import aws

def subframe_iterator_defined(opts):
    return opts.subdiv_x > 0 and opts.subdiv_y > 0

def subframe_iterator(opts):
    if subframe_iterator_defined(opts):
        xfrac = 1.0 / opts.subdiv_x
        yfrac = 1.0 / opts.subdiv_y
        for x in xrange(opts.subdiv_x):
            min_x = x * xfrac
            max_x = (x+1) * xfrac
            for y in xrange(opts.subdiv_y):
                min_y = y * yfrac
                max_y = (y+1) * yfrac
                yield (
                    ('$SF_MIN_X', str(min_x)),
                    ('$SF_MAX_X', str(max_x)),
                    ('$SF_MIN_Y', str(min_y)),
                    ('$SF_MAX_Y', str(max_y)),
                    )

def push(opts, args, conf):
    # get task script
    with open(opts.task_script) as f:
        task_script = f.read()

    # build tasklist
    tasklist = []
    for fnum in xrange(opts.start, opts.end+1, opts.task_size):
        start = fnum
        end = min(fnum + opts.task_size - 1, opts.end)
        framespec = "-s %d -e %d -j 1" % (start, end)
        script = task_script.replace('$FRAME', framespec)
        if subframe_iterator_defined(opts):
            for macro_list in subframe_iterator(opts):
                sf_script = script
                for key, value in macro_list:
                    sf_script = sf_script.replace(key, value)
                tasklist.append(sf_script)
        else:
            tasklist.append(script)

    # possibly randomize the task list
    if opts.randomize:
        random.shuffle(tasklist)

    # get work queue
    q = None
    if not opts.dry_run:
        q = aws.create_sqs_queue(conf)

    # push work queue to sqs
    for task in tasklist:
        print task,
        if q is not None:
            aws.write_sqs_queue(task, q)

def status(opts, args, conf):
    q = aws.get_sqs_queue(conf)
    if q is not None:
        print "Queued tasks:", q.count()

def reset(opts, args, conf):
    q, conn = aws.get_sqs_conn_queue(conf)
    if q:
        if opts.hard:
            conn.delete_queue(q)
        else:
            q.clear()
