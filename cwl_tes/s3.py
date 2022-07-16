import fnmatch
import glob
import io
import os
from typing import IO, Any, List
from urllib.parse import urlparse, urlunparse

from cwltool.stdfsaccess import StdFsAccess
from cwltool.loghandler import _logger

import minio


def is_s3(url):
    """ Check if an URI refers to an S3 resource.

    S3 resources are represented by URIs of the form s3://bucket/key.

    """
    return urlparse(url).scheme == 's3'


def parse_s3_endpoint_url(url):
    """ Parse an S3 endpoint URL into its constituent parts.

    S3 endpoint URLs can be either in "path-style", i.e. of the form::

      https://hostname/bucket/key

    or of the form `s3://bucket/key`. In the latter case, it is assumed
    that we're talking to a bucket hosted on Amazon S3.

    """
    parse = urlparse(url)

    if parse.scheme == 's3':
        netloc = "s3.amazonaws.com"
        insecure = False
    else:
        netloc = parse.netloc
        insecure = parse.scheme == "http"
        url = "s3:/" + str(parse.path)

    return netloc, insecure, url


class S3FsAccess(StdFsAccess):
    """ File system abstraction backed by an S3-like bucket (S3, Minio, ...)

    Based on the FtpFsAccess implementation. This implementation uses the
    Minio client for convenience, but can be used to access resources on
    Amazon S3 and on a Minio server without any difference.

    Access credentials for the bucket are taken from the environment and should
    be set via the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment
    variables.

    """

    def __init__(self, basedir, url, insecure=False):  # type: (str) -> None
        """Perform operations with respect to a base directory."""

        super().__init__(basedir)
        access=os.environ["AWS_ACCESS_KEY_ID"] if "AWS_ACCESS_KEY_ID" in os.environ else ''
        secret=os.environ["AWS_SECRET_ACCESS_KEY"] if "AWS_SECRET_ACCESS_KEY" in os.environ else ''
        if (access=='') or (secret==''):
            raise Exception("AWS credentials not provided")
        print("Endpoint url:" + url)
        self._client = minio.Minio(
            url,
            access_key=access,
            secret_key=secret,
            secure=not insecure,
        )

    def glob(self, pattern):  # type: (str) -> List[str]
        """ Find keys in the bucket that match a given pattern.

        Patterns are of the form::

            s3://bucket/some/key/that/*.has/some/magic/in/it

        """
        _logger.debug("glob() for %s", pattern)

        # Shortcut: we have a pattern without a magic in it. Simply check if
        # the resource exists, and return early.
        if not glob.has_magic(pattern):
            # Single path
            if self.exists(pattern):
                return [pattern]
            else:
                return []

        # We have a magic. Try to isolate a prefix in the path that has no
        # magic in it, and list keys under that prefix, so that we don't have
        # to list and filter the whole bucket, which may be huge.
        bucket, path = _parse_bucket_url(pattern)
        prefix, _ = _split_on_magic(path)

        # List objects under prefix.
        objs = self._client.list_objects(bucket, prefix=prefix, recursive=True)
        keys = [obj.object_name for obj in objs]

        # Filter against given pattern
        matching_keys = fnmatch.filter(keys, path)
        return [_make_bucket_url(bucket, path) for path in matching_keys]

    def open(self, fn, mode):  # type: (str, str) -> IO[Any]
        """ Open a bucket resource for access.
        """
        _logger.debug("open() for %s with mode %s", fn, mode)

        if not is_s3(fn):
            return super().open(fn, mode)

        bucket, relpath = _parse_bucket_url(fn)
        resp = self._client.get_object(bucket, relpath)
        return resp

    def exists(self, fn):  # type: (str) -> bool
        """ Check whether a bucket resource exists.
        """
        _logger.debug("exists() for %s", fn)

        if not is_s3(fn):
            return super().exists(fn)

        bucket, relpath = _parse_bucket_url(fn)
        if not relpath:
            return self._client.bucket_exists(bucket)

        objs = self._client.list_objects(bucket, prefix=relpath)
        paths = [obj.object_name for obj in objs]

        return relpath in paths or relpath + '/' in paths

    def size(self, fn):  # type: (str) -> int
        """ Return the size of a bucket resource (in bytes).
        """
        _logger.debug("size() for %s", fn)

        if not is_s3(fn):
            return super().size(fn)

        bucket, relpath = _parse_bucket_url(fn)
        obj = self._client.stat_object(bucket, relpath)
        return obj.size

    def isfile(self, fn):  # type: (str) -> bool
        """ Check if a bucket resource exists and is a file.

        Note: S3 does not really distinguish between files and directory.
        Consequently, this function (and `isdir`) merely check that the
        given object exists.

        """
        _logger.debug("isfile() for %s", fn)

        if not is_s3(fn):
            return super().isfile(fn)

        return self.exists(fn)

    def isdir(self, fn):  # type: (str) -> bool
        """ Check if a bucket resource exists and is a directory.

        Note: S3 does not really distinguish between files and directory.
        Consequently, this function (and `isfile`) merely check that the
        given object exists.

        """
        _logger.debug("isdir() for %s", fn)

        if not is_s3(fn):
            return super().isdir(fn)

        return self.exists(fn)

    def mkdir(self, url, recursive=True):
        """ Make a directory inside the bucket.

        Note: an S3 directory is simply an object whose name ends in a
        forward slash.

        """
        _logger.debug("mkdir() for %s", url)

        bucket, relpath = _parse_bucket_url(url)
        if not relpath:
            return

        if not relpath.endswith('/'):
            relpath += '/'

        self._client.put_object(bucket, relpath, io.BytesIO(), 0)

    def listdir(self, fn):  # type: (str) -> List[str]
        """ List all objects in a given bucket (non-recursively).
        """
        _logger.debug("listdir() for %s", fn)

        bucket, relpath = _parse_bucket_url(fn)

        prefix = relpath or None
        objs = self._client.list_objects(bucket, prefix=prefix)

        return [
            _make_bucket_url(obj.bucket_name, obj.object_name) for obj in objs
        ]

    def join(self, path, *paths):  # type: (str, *str) -> str
        """ Join bucket paths.
        """
        _logger.debug("join() for %s, %s", path, ', '.join(paths))

        if not is_s3(path):
            return super().join(path, *paths)

        return path + '/' + '/'.join(paths)

    def realpath(self, path):  # type: (str) -> str
        """ Return the real path for a bucket resource.

        This is a no-op.

        """
        _logger.debug("realpath() for %s", path)

        if not is_s3(path):
            return super().realpath(path)

        return path

    def upload(self, handle, fn):
        """ Upload a resource to the bucket.

        The resource is identified by an open file handle.

        """
        _logger.debug("upload() for %s", fn)

        bucket, relpath = _parse_bucket_url(fn)

        nbytes = os.fstat(handle.fileno()).st_size
        self._client.put_object(bucket, relpath, handle, nbytes)


def _parse_bucket_url(url):
    """Parses a bucket URI of the form s3://bucket/path/into/bucket
    """
    parse = urlparse(url)
    return parse.netloc, parse.path[1:]


def _make_bucket_url(bucket, path):
    return urlunparse(("s3", bucket, path, None, None, None))


def _split_on_magic(path):
    """ Split path into two parts, so that the second part starts with a glob.

    If the path does not contain a magic, the second part is empty.

    """
    parts = path.split('/')
    for i, part in enumerate(parts):
        if glob.has_magic(part):
            break
    return '/'.join(parts[:i]), '/'.join(parts[i:])
