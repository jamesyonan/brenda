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

import os, time, datetime, calendar
import boto, boto.sqs, boto.s3, boto.ec2
import boto.utils
import paracurl
from brenda import utils
from brenda.error import ValueErrorRetry
from brenda.ami import AMI_ID

def aws_creds(conf):
    return {
        'aws_access_key_id' : conf['AWS_ACCESS_KEY'],
        'aws_secret_access_key' : conf['AWS_SECRET_KEY'],
        }

def get_s3_conn(conf):
    region = conf.get('S3_REGION')
    if region:
        conn = boto.s3.connect_to_region(region, **aws_creds(conf))
        if not conn:
            raise ValueErrorRetry("Could not establish S3 connection to region %r" % (region,))
    else:
        conn = boto.connect_s3(**aws_creds(conf))
    return conn

def get_sqs_conn(conf):
    region = conf.get('SQS_REGION')
    if region:
        conn = boto.sqs.connect_to_region(region, **aws_creds(conf))
        if not conn:
            ValueErrorRetry("Could not establish SQS connection to region %r" % (region,))
    else:
        conn = boto.connect_sqs(**aws_creds(conf))
    return conn

def get_ec2_conn(conf):
    region = conf.get('EC2_REGION')
    if region:
        conn = boto.ec2.connect_to_region(region, **aws_creds(conf))
        if not conn:
            ValueErrorRetry("Could not establish EC2 connection to region %r" % (region,))
    else:
        conn = boto.connect_ec2(**aws_creds(conf))
    return conn

def parse_s3_url(url):
    if url.startswith('s3://'):
        return url[5:].split('/', 1)

def s3_get(conf, s3url, dest, etag=None):
    """
    High-speed download from S3 that can use multiple simultaneous
    download threads to optimize the downloading of a single file.
    S3 file is given in s3url (using s3://BUCKET/FILE naming
    convention) and will be saved in dest.  If etag from previous
    download is provided, and file hasn't changed since then, don't
    download the file and instead raise an exception of type
    paracurl.Exception where the first element of the exception
    tuple == paracurl.PC_ERR_ETAG_MATCH.  Returns tuple of
    (file_length, etag).
    """
    paracurl_kw = {
        'max_threads' : int(conf.get('CURL_MAX_THREADS', '16')),
        'n_retries' : int(conf.get('CURL_N_RETRIES', '4')),
        'debug' : int(conf.get('CURL_DEBUG', '1'))
        }
    if etag:
        paracurl_kw['etag'] = etag
    s3tup = parse_s3_url(s3url)
    if not s3tup or len(s3tup) != 2:
        raise ValueError("s3_get: bad s3 url: %r" % (s3url,))
    conn = get_s3_conn(conf)
    buck = conn.get_bucket(s3tup[0])
    k = boto.s3.key.Key(buck)
    k.key = s3tup[1]
    url = k.generate_url(600, force_http=True)
    return paracurl.download(dest, url, **paracurl_kw)

def put_s3_file(bucktup, path, s3name):
    """
    bucktup is the return tuple of get_s3_output_bucket_name
    """
    k = boto.s3.key.Key(bucktup[0])
    k.key = bucktup[1][1] + s3name
    k.set_contents_from_filename(path, reduced_redundancy=True)

def format_s3_url(bucktup, s3name):
    """
    bucktup is the return tuple of get_s3_output_bucket_name
    """
    return "s3://%s/%s%s" % (bucktup[1][0], bucktup[1][1], s3name)

def get_s3_output_bucket_name(conf):
    bn = conf.get('RENDER_OUTPUT')
    if not bn:
        raise ValueError("RENDER_OUTPUT not defined in configuration")
    bn = parse_s3_url(bn)
    if not bn:
        raise ValueError("RENDER_OUTPUT must be an s3:// URL")
    if len(bn) == 1:
        bn.append('')
    elif len(bn) == 2 and bn[1] and bn[1][-1] != '/':
        bn[1] += '/'
    return bn

def get_s3_output_bucket(conf):
    bn = get_s3_output_bucket_name(conf)
    conn = get_s3_conn(conf)
    buck = conn.get_bucket(bn[0])
    return buck, bn

def parse_sqs_url(url):
    if url.startswith('sqs://'):
        return url[6:]

def get_sqs_work_queue_name(conf):
    qname = conf.get('WORK_QUEUE')
    if not qname:
        raise ValueError("WORK_QUEUE not defined in configuration")
    qname = parse_sqs_url(qname)
    if not qname:
        raise ValueError("WORK_QUEUE must be an sqs:// URL")
    return qname

def create_sqs_queue(conf):
    visibility_timeout = int(conf.get('VISIBILITY_TIMEOUT', '120'))
    qname = get_sqs_work_queue_name(conf)
    conn = get_sqs_conn(conf)
    return conn.create_queue(qname, visibility_timeout=visibility_timeout)

def get_sqs_conn_queue(conf):
    qname = get_sqs_work_queue_name(conf)
    conn = get_sqs_conn(conf)
    return conn.get_queue(qname), conn

def get_sqs_queue(conf):
    return get_sqs_conn_queue(conf)[0]

def write_sqs_queue(string, queue):
    m = boto.sqs.message.Message()
    m.set_body(string)
    queue.write(m)

def get_ec2_instances(conf):
    conn = get_ec2_conn(conf)
    reservations = conn.get_all_instances()
    return [i for r in reservations for i in r.instances]

def format_uptime(sec):
    return str(datetime.timedelta(seconds=sec))

def get_uptime(now, aws_launch_time):
    lt = boto.utils.parse_ts(aws_launch_time)
    return int(now - calendar.timegm(lt.timetuple()))

def filter_instances(opts, conf, hostset=None):
    def threshold_test(aws_launch_time):
        ut = get_uptime(now, aws_launch_time)
        return (ut / 60) % 60 >= opts.threshold

    now = time.time()
    ami = utils.get_opt(opts.ami, conf, 'AMI_ID', default=AMI_ID)
    if hostset is None:
        if getattr(opts, 'hosts_file', None):
            with open(opts.hosts_file, 'r') as f:
                hostset = frozenset([line.strip() for line in f.readlines()])
        elif getattr(opts, 'host', None):
            hostset = frozenset((opts.host,))
    inst = [i for i in get_ec2_instances(conf)
            if i.image_id and i.public_dns_name
            and threshold_test(i.launch_time)
            and (ami is None or ami == i.image_id)
            and (hostset is None or i.public_dns_name in hostset)]
    inst.sort(key = lambda i : (i.image_id, i.launch_time, i.public_dns_name))
    return inst

def shutdown_by_public_dns_name(opts, conf, dns_names):
    iids = []
    for i in get_ec2_instances(conf):
        if i.public_dns_name in dns_names:
            iids.append(i.id)
    shutdown(opts, conf, iids)

def shutdown(opts, conf, iids):
    if opts.terminate:
        print "TERMINATE", iids
        if not opts.dry_run:
            conn = get_ec2_conn(conf)
            conn.terminate_instances(instance_ids=iids)
    else:
        print "SHUTDOWN", iids
        if not opts.dry_run:
            conn = get_ec2_conn(conf)
            conn.stop_instances(instance_ids=iids)

def get_ssh_pubkey_fn(opts, conf):
    v = conf.get('SSH_PUBKEY')
    if not v:
        v = os.path.join(os.path.expanduser("~"), '.ssh', 'id_rsa.pub')
    return v

def get_ssh_identity_fn(opts, conf):
    v = conf.get('SSH_IDENTITY')
    if not v:
        v = os.path.join(os.path.expanduser("~"), '.ssh', 'id_rsa')
    return v

def get_default_ami_with_fmt(fmt):
    if AMI_ID:
        return fmt % (AMI_ID,)
    else:
        return ""
