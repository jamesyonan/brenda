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

import os, sys, signal, subprocess, multiprocessing, stat, time
import paracurl
from brenda import aws, utils, error

class State(object):
    pass

# We use subprocess.Popen (for blender task) and
# multiprocessing.Process (for S3-push task) polymorphically,
# so add some methods to make them consistent.

class Subprocess(subprocess.Popen):
    def stop(self):
        self.terminate()
        return self.wait()

class Multiprocess(multiprocessing.Process):
    def stop(self):
        if self.is_alive():
            self.terminate()
            self.join()
        return self.exitcode

    def poll(self):
        return self.exitcode

def start_s3_push_process(opts, args, conf, outdir):
    p = Multiprocess(target=s3_push_process, args=(opts, args, conf, outdir))
    p.start()
    return p

def s3_push_process(opts, args, conf, outdir):
    def do_s3_push():
        bucktup = aws.get_s3_output_bucket(conf)
        for dirpath, dirnames, filenames in os.walk(outdir):
            for f in filenames:
                path = os.path.join(dirpath, f)
                print "PUSH", path, "TO", aws.format_s3_url(bucktup, f)
                aws.put_s3_file(bucktup, path, f)
            break

    try:
        error.retry(conf, do_s3_push)
    except Exception, e:
        print "S3 push failed:", e
        sys.exit(1)
    sys.exit(0)

def run_tasks(opts, args, conf):
    def write_done_file():
        with open("DONE", "w") as f:
            f.write(aws.get_done(opts, conf)+'\n')

    def read_done_file():
        try:
            with open('DONE') as f:
                ret = f.readline().strip()
        except:
            ret = 'exit'
        aws.validate_done(ret)
        return ret

    def task_complete_accounting(task_count):
        # update some info files if we are running in daemon mode
        # number of tasks we have completed so far
        utils.write_atomic('task_count', "%d\n" % (task_count,))

        # timestamp of completion of last task
        utils.write_atomic('task_last', "%d\n" % (time.time(),))

    def signal_handler(signal, frame):
        print "******* SIGNAL %r, exiting" % (signal,)
        cleanup_all()
        sys.exit(1)

    def cleanup_all():
        tasks = (local.task_active, local.task_push)
        local.task_active = local.task_push = None
        for i, task in enumerate(tasks):
            name = task_names[i]
            cleanup(task, name)

    def cleanup(task, name):
        if task:
            if task.msg is not None:
                try:
                    msg = task.msg
                    task.msg = None
                    msg.change_visibility(0) # immediately return task back to work queue
                except Exception, e:
                    print "******* CLEANUP EXCEPTION sqs change_visibility", name, e
            if task.proc is not None:
                try:
                    proc = task.proc
                    task.proc = None
                    proc.stop()
                except Exception, e:
                    print "******* CLEANUP EXCEPTION proc stop", name, e
            if task.outdir is not None:
                try:
                    outdir = task.outdir
                    task.outdir = None
                    utils.rmtree(outdir)
                except Exception, e:
                    print "******* CLEANUP EXCEPTION rm outdir", name, task.outdir, e

    def task_loop():
        try:
            # reset tasks
            local.task_active = None
            local.task_push = None

            # get SQS work queue
            q = aws.get_sqs_queue(conf)

            # Loop over tasks.  There are up to two different tasks at any
            # given moment that we are processing concurrently:
            #
            # 1. Active task -- usually a blender render operation.
            # 2. S3 push task -- a task which pushes the products of the
            #                    previous active task (such as rendered
            #                    frames) to S3.
            while True:
                # reset active task
                local.task_active = None

                # initialize active task object
                task = State()
                task.msg = None
                task.proc = None
                task.retcode = None
                task.outdir = None
                task.id = 0

                # Get a task from the SQS work queue.  This is normally
                # a short script that runs blender to render one
                # or more frames.
                task.msg = q.read()

                # output some debug info
                print "queue read:", task.msg
                if local.task_push:
                    print "push task:", local.task_push.__dict__
                else:
                    print "no task push task"

                # process task
                if task.msg is not None:
                    # assign an ID to task
                    local.task_id_counter += 1
                    task.id = local.task_id_counter

                    # register active task
                    local.task_active = task

                    # create output directory
                    task.outdir = os.path.join(work_dir, "brenda-outdir%d.tmp" % (task.id,))
                    utils.rmtree(task.outdir)
                    utils.mkdir(task.outdir)

                    # get the task script
                    script = task.msg.get_body()
                    print "script len:", len(script)

                    # do macro substitution on the task script
                    script = script.replace('$OUTDIR', task.outdir)

                    # add shebang if absent
                    if not script.startswith("#!"):
                        script = "#!/bin/bash\n" + script

                    # cd to project directory, where we will run blender from
                    with utils.Cd(proj_dir) as cd:
                        # write script file and make it executable
                        script_fn = "./brenda-go"
                        with open(script_fn, 'w') as f:
                            f.write(script)
                        st = os.stat(script_fn)
                        os.chmod(script_fn, st.st_mode | (stat.S_IEXEC|stat.S_IXGRP|stat.S_IXOTH))

                        # run the script
                        print "------- Run script %s -------" % (os.path.realpath(script_fn),)
                        print script,
                        print "--------------------------"
                        task.proc = Subprocess([script_fn])

                    print "active task:", local.task_active.__dict__

                # Wait for active and S3-push tasks to complete,
                # while periodically reasserting with SQS to
                # acknowledge that tasks are still pending.
                # (If we don't reassert with SQS frequently enough,
                # it will assume we died, and put our tasks back
                # in the queue.  "frequently enough" means within
                # visibility_timeout.)
                count = 0
                while True:
                    reassert = (count >= visibility_timeout_reassert)
                    for i, task in enumerate((local.task_active, local.task_push)):
                        if task:
                            name = task_names[i]
                            if task.proc is not None:
                                # test if process has finished
                                task.retcode = task.proc.poll()
                                if task.retcode is not None:
                                    # process has finished
                                    task.proc = None

                                    # did process finish with errors?
                                    if task.retcode != 0:
                                        errtxt = "fatal error in %s task" % (name,)
                                        if name == 'active':
                                            raise error.ValueErrorRetry(errtxt)
                                        else:
                                            raise ValueError(errtxt)

                                    # Process finished successfully.  If S3-push process,
                                    # tell SQS that the task completed successfully.
                                    if name == 'push':
                                        print "******* TASK", task.id, "COMMITTED to S3"
                                        q.delete_message(task.msg)
                                        task.msg = None
                                        local.task_count += 1
                                        task_complete_accounting(local.task_count)

                                    # active task completed?
                                    if name == 'active':
                                        print "******* TASK", task.id, "READY-FOR-PUSH"

                            # tell SQS that we are still working on the task
                            if reassert and task.proc is not None:
                                print "******* REASSERT", name, task.id
                                task.msg.change_visibility(visibility_timeout)

                    # break out of loop only when no pending tasks remain
                    if ((not local.task_active or local.task_active.proc is None)
                        and (not local.task_push or local.task_push.proc is None)):
                        break

                    # setup for next process poll iteration
                    if reassert:
                        count = 0
                    time.sleep(1)
                    count += 1

                # clean up the S3-push task
                cleanup(local.task_push, 'push')
                local.task_push = None

                # start a concurrent push task to commit files generated by
                # just-completed active task (such as blender render frames) to S3
                if local.task_active:
                    local.task_active.proc = start_s3_push_process(opts, args, conf, local.task_active.outdir)
                    local.task_push = local.task_active
                    local.task_active = None

                # if no active task and no S3-push task, we are done (unless DONE is set to "poll")
                if not local.task_active and not local.task_push:
                    if read_done_file() == "poll":
                        print "Polling for more work..."
                        time.sleep(15)
                    else:
                        break

        finally:
            cleanup_all()

    # initialize task_active and task_push states
    task_names = ('active', 'push')
    local = State()
    local.task_active = None
    local.task_push = None
    local.task_id_counter = 0
    local.task_count = 0

    # setup signal handler
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # get configuration parameters
    work_dir = aws.get_work_dir(conf)
    visibility_timeout_reassert = int(conf.get('VISIBILITY_TIMEOUT_REASSERT', '30'))
    visibility_timeout = int(conf.get('VISIBILITY_TIMEOUT', '120'))

    # validate RENDER_OUTPUT bucket
    aws.get_s3_output_bucket(conf)

    # file cleanup
    utils.rm('task_count')
    utils.rm('task_last')

    # create Blender temporary directory
    tmp_dir = os.path.join(work_dir, 'tmp')
    if not os.path.isdir(tmp_dir):
        utils.mkdir(tmp_dir)
    os.environ['TMP'] = tmp_dir

    # save the value of DONE config var
    write_done_file()

    # Get our spot instance request, if it exists
    spot_request_id = None
    if int(conf.get('RUNNING_ON_EC2', '1')):
        try:
            instance_id = aws.get_instance_id_self()
            spot_request_id = aws.get_spot_request_from_instance_id(conf, instance_id)
            print "Spot request ID:", spot_request_id
        except Exception, e:
            print "Error determining spot instance request:", e

    # get project (from s3:// or file://)
    blender_project = conf.get('BLENDER_PROJECT')
    if not blender_project:
        raise ValueError("BLENDER_PROJECT not defined in configuration")

    # directory that blender will be run from
    proj_dir = get_project(conf, blender_project)
    print "PROJ_DIR", proj_dir

    # mount additional EBS volumes
    aws.mount_additional_ebs(conf, proj_dir)

    # continue only if we are not in "dry-run" mode
    if not opts.dry_run:
        # execute the task loop
        error.retry(conf, task_loop)

        # if "DONE" file == "shutdown", do a shutdown now as we exit
        if read_done_file() == "shutdown":
            if spot_request_id:
                try:
                    # persistent spot instances must be explicitly cancelled, or
                    # EC2 will automatically requeue the spot instance request
                    print "Canceling spot instance request:", spot_request_id
                    aws.cancel_spot_request(conf, spot_request_id)
                except Exception, e:
                    print "Error canceling spot instance request:", e
            utils.shutdown()

        print "******* DONE (%d tasks completed)" % (local.task_count,)

def get_s3_project(conf, s3url, proj_dir):
    # target file in which to save S3 download
    fn = os.path.basename(s3url)

    # Does .etag file exist?  If so, pass etag to
    # s3_get to avoid downloading the file if it
    # already exists.
    always_refetch = int(conf.get('BLENDER_PROJECT_ALWAYS_REFETCH', '0'))
    etag = None
    if not always_refetch:
        try:
            with open(os.path.join(proj_dir, fn + '.etag')) as efn:
                etag = efn.read().strip()
        except Exception, e:
            pass

    # create new directory to download project
    new_dir = proj_dir + '.pre.tmp'
    utils.rmtree(new_dir)
    utils.mkdir(new_dir)

    try:
        with utils.Cd(new_dir) as cd:
            # download the file from S3
            file_len, etag = aws.s3_get(conf, s3url, fn, etag=etag)

            # save the etag for future reference
            with open(fn + '.etag', 'w') as efn:
                efn.write(etag+'\n')

            # Use "unzip" tool for .zip files,
            # and "tar xf" for everything else.
            if fn.lower().endswith('.zip'):
                utils.system(["unzip", fn])
            else:
                utils.system(["tar", "xf", fn])
            utils.rm(fn)

        utils.rmtree(proj_dir)
        utils.mv(new_dir, proj_dir)

    except paracurl.Exception, e:
        if e[0] == paracurl.PC_ERR_ETAG_MATCH:
            # file was previously downloaded, don't need new_dir
            print "Note: retaining previous download of", s3url
        else:
            raise

    finally:
        utils.rmtree(new_dir)

def get_project(conf, url):
    if url.startswith("file://"):
        path = url[7:]
        if not os.path.isdir(path):
            raise ValueError("%s does not point to a directory" % (url,))
        return path
    else:
        work_dir = aws.get_work_dir(conf)
        ebs_snap = aws.project_ebs_snapshot(conf)
        if ebs_snap:
            proj_dir = os.path.join(work_dir, "brenda-project.mount")
            dev = utils.blkdev(0, mount_form=True)
            utils.mount(dev, proj_dir)
        else:
            proj_dir = os.path.join(work_dir, "brenda-project.tmp")
            get_s3_project(conf, url, proj_dir)
        return utils.top_dir(proj_dir)
