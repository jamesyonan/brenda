/*
 * Paracurl -- a library for high-speed downloads of a URL using
 * multiple threads
 *
 * Copyright (C) 2013 James Yonan <james@openvpn.net>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in
 * the documentation and/or other materials provided with the
 * distribution.  The names of contributors to this software
 * may not be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 * COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <Python.h>
#include <curl/curl.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>

#if !defined(_FILE_OFFSET_BITS) || _FILE_OFFSET_BITS < 64
#error _FILE_OFFSET_BITS must be at least 64
#endif

#define PC_ERR_ALLOC          -1
#define PC_ERR_ASSERT         -2
#define PC_ERR_CONTENT_LEN    -3
#define PC_ERR_MAX_THREADS    -4
#define PC_ERR_HTTP           -5
#define PC_ERR_OPEN           -6
#define PC_ERR_LSEEK          -7
#define PC_ERR_CLOSE          -8
#define PC_ERR_BYTES_WRITTEN  -9
#define PC_ERR_PTHREAD_CREATE -10
#define PC_ERR_PTHREAD_JOIN   -11
#define PC_ERR_RENAME         -12
#define PC_ERR_ETAG_MATCH     -13

#define OPEN_MODE (S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH)

struct paracurl_state;

struct paracurl_thread {
	pthread_t thread;
	size_t index;
	off_t range_start;
	off_t range_len;
	off_t bytes_written;
	int fd;
	int status;
	struct paracurl_state *state;	/* backref to parent */
	char cacheline_pad[64];
};

struct paracurl_state {
	char *outpath;
	char *outpath_tmp;
	char *url;
	char *url_etag;
	char *etag;
	size_t max_threads;
	size_t n_retries;
	unsigned int retry_pause;
	long timeout;
	long connect_timeout;
	int debug;

	off_t content_length;

	size_t n_threads;
	struct paracurl_thread *threads;
};

static PyObject *paracurl_Exception;

static int string_alloc(char **dest, const char *src)
{
	if (src) {
		char *s = strdup(src);
		if (s) {
			*dest = s;
			return 0;
		} else
			return 1;
	} else {
		*dest = NULL;
		return 0;
	}
}

static const char *error_string(const int code)
{
	if (code > 0) {
		/* positive error codes are CURL errors */
		return curl_easy_strerror(code);
	} else {
		/* negative error codes are PC_ERR_x errors */
		switch (code) {
		case 0:
			return "OK";
		case PC_ERR_ALLOC:
			return "PC_ERR_ALLOC: memory allocation failure";
		case PC_ERR_ASSERT:
			return "PC_ERR_ASSERT: assertion failed";
		case PC_ERR_CONTENT_LEN:
			return "PC_ERR_CONTENT_LEN : content-length is undefined or 0";
		case PC_ERR_MAX_THREADS:
			return "PC_ERR_MAX_THREADS : bad max_threads value";
		case PC_ERR_HTTP:
			return "PC_ERR_HTTP : bad HTTP status code";
		case PC_ERR_OPEN:
			return "PC_ERR_OPEN : error opening output file";
		case PC_ERR_LSEEK:
			return "PC_ERR_LSEEK : error seeking on output file";
		case PC_ERR_CLOSE:
			return "PC_ERR_CLOSE : error closing output file";
		case PC_ERR_BYTES_WRITTEN:
			return "PC_ERR_BYTES_WRITTEN : wrong number of bytes written to file segment";
		case PC_ERR_PTHREAD_CREATE:
			return "PC_ERR_PTHREAD_CREATE : error creating thread";
		case PC_ERR_PTHREAD_JOIN:
			return "PC_ERR_PTHREAD_JOIN : error joining thread";
		case PC_ERR_RENAME:
			return "PC_ERR_RENAME : error renaming output file";
		case PC_ERR_ETAG_MATCH:
			return "PC_ERR_ETAG_MATCH : ETag match, not downloaded";
		default:
			return "PC_ERR_??? : unknown error";
		}
	}
}

static int init_thread_info(struct paracurl_state *state)
{
	const off_t minseg = 1000000;
	size_t n_threads;
	off_t seg;
	off_t x, len;
	size_t i;

	if (state->max_threads <= 0)
		return PC_ERR_MAX_THREADS;
	seg = state->content_length / state->max_threads;
	if (seg >= minseg)
		n_threads = state->max_threads;
	else {
		seg = minseg;
		n_threads = (size_t)(state->content_length / minseg + 1);
	}
	state->threads = (struct paracurl_thread *) calloc(n_threads, sizeof(struct paracurl_thread));
	if (!state->threads)
		return PC_ERR_ALLOC;
	x = 0;
	for (i = 0; i < n_threads; ++i) {
		struct paracurl_thread *thr = &state->threads[i];
		len = seg;
		if (i == n_threads - 1)
			len = state->content_length - x;
		if (!len)
			return PC_ERR_ASSERT;
		thr->index = i;
		thr->state = state;
		thr->range_start = x;
		thr->range_len = len;
		thr->fd = -1;
		if (state->debug >= 2)
			printf("SEG[%zd] off=%lld len=%lld\n", i, (long long) x, (long long) len);
		x += len;
	}
	if (x != state->content_length)
		return PC_ERR_ASSERT;
	state->n_threads = n_threads;
	return 0;
}

static size_t write_data_null(void *ptr, size_t size, size_t nmemb, void *userdata)
{
	size *= nmemb;
	return size;
}

static size_t write_data_segment(void *ptr, size_t size, size_t nmemb, void *userdata)
{
	struct paracurl_thread *thr = (struct paracurl_thread *) userdata;
	ssize_t actsize;

	size *= nmemb;
	if (thr->bytes_written + size > thr->range_len)
		size = (size_t)(thr->range_len - thr->bytes_written);	/* don't overflow segment */
	actsize = write(thr->fd, ptr, size);
	if (actsize == -1)
		return 0;
	thr->bytes_written += actsize;
	return actsize;
}

static size_t header_callback(void *ptr, size_t size, size_t nmemb, void *userdata)
{
	static const char cr_prefix[] = "Content-Range: bytes 0-0/";
	static const char etag_prefix[] = "ETag: \"";

	struct paracurl_state *state = (struct paracurl_state *) userdata;
	char *header = NULL;

	size *= nmemb;

	/* header data not guaranteed by curl to be null-terminated */
	header = (char *) malloc(size + 1);
	if (!header)
		return 0;
	memcpy(header, ptr, size);
	header[size] = '\0';

	/* look for Content-Range header */
	if (strncasecmp(header, cr_prefix, sizeof(cr_prefix) - 1) == 0)
		state->content_length = atoll(header + sizeof(cr_prefix) - 1);

	/* look for Etag header */
	if (strncasecmp(header, etag_prefix, sizeof(etag_prefix) - 1) == 0) {
		const char *etag = header + sizeof(etag_prefix) - 1;
		const size_t len = strlen(etag);
		char *dest = (char *) malloc(len + 1);
		char *trailquote;

		if (dest) {
			memcpy(dest, etag, len + 1);
			trailquote = strchr(dest, '\"');
			if (trailquote) {
				*trailquote = '\0';
				state->url_etag = dest;
			} else
				free(dest);
		}
	}

	if (state->debug == 2)
		printf("%s", header);
	if (header)
		free(header);
	return size;
}

static void *thread_func(void *userdata)
{
	struct paracurl_thread *thr = (struct paracurl_thread *) userdata;
	const struct paracurl_state *state = thr->state;
	CURL *curl_handle = NULL;
	int status = 0;
	size_t i;
	char content_range[64];

	/* open output file  */
	thr->fd = open(state->outpath_tmp, O_WRONLY, OPEN_MODE);
	if (thr->fd == -1) {
		status = PC_ERR_OPEN;
		goto done;
	}

	/* format content-range */
	snprintf(content_range, sizeof(content_range), "%lld-%lld",
			 (long long) thr->range_start,
			 (long long) thr->range_start + thr->range_len - 1);

	/* read segment of file that has been assigned to this thread */
	for (i = 0; i < state->n_retries; ++i) {
		if (curl_handle)
			curl_easy_cleanup(curl_handle);
		if (i) {
			if (state->retry_pause)
				sleep(state->retry_pause);
			if (state->debug >= 1)
				printf("RETRY[%zd] %zd/%zd status=%d (%s)\n",
					   thr->index, i + 1, state->n_retries,
					   status, error_string(status));
		}
		curl_handle = curl_easy_init();
		if (!curl_handle) {
			status = PC_ERR_ALLOC;
			break;
		}

		/* bytes written so far */
		thr->bytes_written = 0;

		/* seek to correct position for this segment */
		{
			const off_t pos = lseek(thr->fd, thr->range_start, SEEK_SET);
			if (pos != thr->range_start) {
				status = PC_ERR_LSEEK;
				break;
			}
		}

		/* get content */
		curl_easy_setopt(curl_handle, CURLOPT_URL, state->url);
		curl_easy_setopt(curl_handle, CURLOPT_RANGE, content_range);
		curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, write_data_segment);
		curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, thr);
		curl_easy_setopt(curl_handle, CURLOPT_NOSIGNAL, 1);
		if (state->timeout)
			curl_easy_setopt(curl_handle, CURLOPT_TIMEOUT, state->timeout);
		if (state->connect_timeout)
			curl_easy_setopt(curl_handle, CURLOPT_CONNECTTIMEOUT, state->connect_timeout);
		if (state->debug >= 3)
			curl_easy_setopt(curl_handle, CURLOPT_VERBOSE, 1);
		status = curl_easy_perform(curl_handle);
		if (state->debug >= 2)
			printf("curl_easy_perform[%zd] status=%d (%s)\n",
				   thr->index, status, error_string(status));
		if (status) {
			if (status == CURLE_WRITE_ERROR)
				break;
			continue;
		}

		/* check HTTP return code */
		{
			long http_code = 0;
			curl_easy_getinfo(curl_handle, CURLINFO_RESPONSE_CODE, &http_code);
			if (state->debug >= 2)
				printf("HTTP response code %ld\n", http_code);
			if (http_code != 206) {	/* HTTP/1.1 206 Partial Content */
				status = PC_ERR_HTTP;
				continue;
			}
		}

		/* check that bytes_written is correct */
		if (thr->bytes_written != thr->range_len) {
			status = PC_ERR_BYTES_WRITTEN;
			continue;
		}

		/* good */
		break;
	}

	/* if status is nonzero, failed after n retries */
	if (status)
		goto done;

	/* log success */
	if (state->debug >= 1)
		printf("WRITE[%zd] %s\n", thr->index, content_range);

      done:
	if (curl_handle)
		curl_easy_cleanup(curl_handle);
	if (thr->fd != -1) {
		if (close(thr->fd))
			thr->status = PC_ERR_CLOSE;
	}
	if (status)
		thr->status = status;
	return NULL;
}

static PyObject *paracurl_download(PyObject * self, PyObject * args,
				   PyObject * kwargs)
{
	char *outpath_arg = NULL;
	char *url_arg = NULL;
	char *etag_arg = NULL;
	int max_threads_arg = 16;
	int n_retries_arg = 5;
	int retry_pause_arg = 5;
	int timeout_arg = 0;
	int connect_timeout_arg = 60;
	int debug_arg = 0;

	PyThreadState *py_thread_state = NULL;
	struct paracurl_state *state = NULL;
	CURL *curl_handle = NULL;
	int status = 0;
	size_t i;

	static char *kwlist[] = {
		/* REQUIRED parameters */
		"outpath",	/* save download to this file */
		"url",		/* URL to download */

		/* OPTIONAL parameters */
		"etag",		/* if URL Etag matches this string, dont't download and return PC_ERR_ETAG_MATCH */
		"max_threads",	/* maximum number of download threads */
		"n_retries",	/* number of retries on curl failure */
		"retry_pause",	/* number of seconds to pause before retry */
		"timeout",	    /* curl download timeout */
		"connect_timeout",	/* curl connection timeout */
		"debug",	    /* debug level */
		NULL
	};

	/* parse args */
	if (!PyArg_ParseTupleAndKeywords
	    (args, kwargs, "ss|ziiiiii:paracurl.download", kwlist,
	     &outpath_arg, &url_arg, &etag_arg, &max_threads_arg,
	     &n_retries_arg, &retry_pause_arg, &timeout_arg,
	     &connect_timeout_arg, &debug_arg))
		return NULL;

	/* initialize state */
	state = (struct paracurl_state *) calloc(1, sizeof(struct paracurl_state));
	if (!state) {
		status = PC_ERR_ALLOC;
		goto done;
	}

	/* initialize strings */
	{
		int errs = 0;
		errs |= string_alloc(&state->outpath, outpath_arg);
		errs |= string_alloc(&state->url, url_arg);
		errs |= string_alloc(&state->etag, etag_arg);
		if (errs) {
			status = PC_ERR_ALLOC;
			goto done;
		}
	}

	/* initialize numerical values */
	state->max_threads = max_threads_arg;
	state->n_retries = n_retries_arg;
	state->retry_pause = retry_pause_arg;
	state->timeout = timeout_arg;
	state->connect_timeout = connect_timeout_arg;
	state->debug = debug_arg;

	/* save python thread state */
	py_thread_state = PyEval_SaveThread();

	/* test that URL is gettable and fetch Content-Length */
	for (i = 0; i < state->n_retries; ++i) {
		if (curl_handle)
			curl_easy_cleanup(curl_handle);
		if (i) {
			if (state->retry_pause)
				sleep(state->retry_pause);
			if (state->debug >= 1)
				printf("RETRY %zd/%zd status=%d (%s)\n",
				       i + 1, state->n_retries, status,
				       error_string(status));
		}
		curl_handle = curl_easy_init();
		if (!curl_handle) {
			status = PC_ERR_ALLOC;
			break;
		}

		/* get content length */
		curl_easy_setopt(curl_handle, CURLOPT_URL, state->url);
		curl_easy_setopt(curl_handle, CURLOPT_RANGE, "0-0");
		curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, write_data_null);
		curl_easy_setopt(curl_handle, CURLOPT_HEADERFUNCTION, header_callback);
		curl_easy_setopt(curl_handle, CURLOPT_WRITEHEADER, state);
		curl_easy_setopt(curl_handle, CURLOPT_NOSIGNAL, 1);
		if (state->connect_timeout) {
			curl_easy_setopt(curl_handle, CURLOPT_CONNECTTIMEOUT, state->connect_timeout);
			curl_easy_setopt(curl_handle, CURLOPT_TIMEOUT, state->connect_timeout);
		}
		if (state->debug >= 3)
			curl_easy_setopt(curl_handle, CURLOPT_VERBOSE, 1);
		status = curl_easy_perform(curl_handle);
		if (state->debug >= 2) {
			printf("curl_easy_perform status=%d (%s) content_len=%lld etag=%s\n",
				   status, error_string(status),
				   (long long) state->content_length,
				   state->url_etag ? state->url_etag : "NULL");
		}
		if (status)
			continue;

		/* check HTTP return code */
		{
			long http_code = 0;
			curl_easy_getinfo(curl_handle, CURLINFO_RESPONSE_CODE, &http_code);
			if (state->debug >= 2)
				printf("HTTP response code %ld\n", http_code);
			if (http_code != 206) {	/* HTTP/1.1 206 Partial Content */
				status = PC_ERR_HTTP;
				continue;
			}
		}

		/* check content length */
		if (state->content_length)
			break;	/* good */
		else
			status = PC_ERR_CONTENT_LEN;
	}

	/* if status is nonzero, failed after n retries */
	if (status)
		goto done;

	/* cleanup curl context */
	curl_easy_cleanup(curl_handle);
	curl_handle = NULL;

	/* if Etag of URL resource and passed etag parameter matches, no need to download */
	if (state->etag && state->url_etag && strcmp(state->etag, state->url_etag) == 0) {
		status = PC_ERR_ETAG_MATCH;
		goto done;
	}

	/* generate temporary output filename and verify that file is writable & seekable */
	{
		int fd;
		const size_t fnsiz = strlen(state->outpath) + 16;
		state->outpath_tmp = (char *) malloc(fnsiz);
		snprintf(state->outpath_tmp, fnsiz, "%s.tmp",
			 state->outpath);
		if (state->debug >= 2)
			printf("outpath_tmp=%s\n", state->outpath_tmp);
		fd = open(state->outpath_tmp, O_CREAT | O_TRUNC | O_WRONLY,
			  OPEN_MODE);
		if (fd == -1) {
			status = PC_ERR_OPEN;
			goto done;
		}
		const off_t pos = lseek(fd, state->content_length, SEEK_SET);
		if (pos != state->content_length) {
			status = PC_ERR_LSEEK;
			goto done;
		}
		if (close(fd)) {
			status = PC_ERR_CLOSE;
			goto done;
		}
	}

	/* break file into segments for threads */
	status = init_thread_info(state);
	if (status)
		goto done;

	/* print URL */
	if (state->debug >= 1)
		printf("GET %s\n", state->url);

	/* fire up threads */
	for (i = 0; i < state->n_threads; ++i) {
		struct paracurl_thread *thr = &state->threads[i];
		if (pthread_create(&thr->thread, NULL, thread_func, thr))
			thr->status = PC_ERR_PTHREAD_CREATE;
	}

	/* wait for threads to finish */
	for (i = 0; i < state->n_threads; ++i) {
		struct paracurl_thread *thr = &state->threads[i];
		if (thr->status != PC_ERR_PTHREAD_CREATE && pthread_join(thr->thread, NULL))
			status = PC_ERR_PTHREAD_JOIN;
		else if (thr->status)
			status = thr->status;
	}

	/* succeed */
	if (!status && rename(state->outpath_tmp, state->outpath))
		status = PC_ERR_RENAME;

  done:
	{
		PyObject *ret = NULL;
		if (py_thread_state)
			PyEval_RestoreThread(py_thread_state);
		if (curl_handle)
			curl_easy_cleanup(curl_handle);
		if (state) {
			if (!status)
				ret = Py_BuildValue("(Ls)", (long long) state->content_length, state->url_etag);
			if (state->outpath_tmp) {
				if (status)
					unlink(state->outpath_tmp);
				free(state->outpath_tmp);
			}
			free(state->outpath);
			free(state->url);
			free(state->url_etag);
			free(state->etag);
			free(state->threads);
			free(state);
		}
		if (status) {
			PyObject *err_tup = Py_BuildValue("(is)", status, error_string(status));
			PyErr_SetObject(paracurl_Exception, err_tup);
			Py_XDECREF(err_tup);
		} else if (!ret) {
			Py_INCREF(Py_None);
			ret = Py_None;
		}
		return ret;
	}
}

static PyMethodDef paracurl_methods[] = {
	{"download", (PyCFunction) paracurl_download, METH_VARARGS | METH_KEYWORDS, "Download a URL using parallel threads"},
	{NULL}
};

void initparacurl(void)
{
	/* initialize paracurl module */
	PyObject *m = Py_InitModule3("paracurl", paracurl_methods, "Download a URL using parallel threads");
	paracurl_Exception = PyErr_NewException("paracurl.Exception", NULL, NULL);
	PyModule_AddObject(m, "Exception", paracurl_Exception);

	PyModule_AddIntConstant(m, "PC_ERR_ALLOC", PC_ERR_ALLOC);
	PyModule_AddIntConstant(m, "PC_ERR_ASSERT", PC_ERR_ASSERT);
	PyModule_AddIntConstant(m, "PC_ERR_CONTENT_LEN", PC_ERR_CONTENT_LEN);
	PyModule_AddIntConstant(m, "PC_ERR_MAX_THREADS", PC_ERR_MAX_THREADS);
	PyModule_AddIntConstant(m, "PC_ERR_HTTP", PC_ERR_HTTP);
	PyModule_AddIntConstant(m, "PC_ERR_OPEN", PC_ERR_OPEN);
	PyModule_AddIntConstant(m, "PC_ERR_LSEEK", PC_ERR_LSEEK);
	PyModule_AddIntConstant(m, "PC_ERR_CLOSE", PC_ERR_CLOSE);
	PyModule_AddIntConstant(m, "PC_ERR_BYTES_WRITTEN", PC_ERR_BYTES_WRITTEN);
	PyModule_AddIntConstant(m, "PC_ERR_PTHREAD_CREATE", PC_ERR_PTHREAD_CREATE);
	PyModule_AddIntConstant(m, "PC_ERR_PTHREAD_JOIN", PC_ERR_PTHREAD_JOIN);
	PyModule_AddIntConstant(m, "PC_ERR_RENAME", PC_ERR_RENAME);
	PyModule_AddIntConstant(m, "PC_ERR_ETAG_MATCH", PC_ERR_ETAG_MATCH);
}
