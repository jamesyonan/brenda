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

import time, httplib, socket
import boto.exception

class ValueErrorRetry(ValueError):
    """
    This exception should be raised for recoverable
    errors, where there is a reasonable assumption that
    retrying the operation will succeed.
    """
    pass

def retry(conf, action):
    n_retries = int(conf.get('N_RETRIES', '5'))
    reset_period = int(conf.get('RESET_PERIOD', '3600'))
    error_pause = int(conf.get('ERROR_PAUSE', '30'))

    reset = int(time.time())
    i = 0
    while True:
        try:
            ret = action()
        # These are the exception types that justify a retry -- extend this list as needed
        except (httplib.IncompleteRead, socket.error, boto.exception.BotoClientError, ValueErrorRetry), e:
            now = int(time.time())
            if now > reset + reset_period:
                print "******* RETRY RESET"
                i = 0
                reset = now
            i += 1
            print "******* RETRY %d/%d: %s" % (i, n_retries, e)
            if i < n_retries:
                print "******* WAITING %d seconds..." % (error_pause,)
                time.sleep(error_pause)
            else:
                raise ValueError("FAIL after %d retries" % (n_retries,))
        else:
            return ret
