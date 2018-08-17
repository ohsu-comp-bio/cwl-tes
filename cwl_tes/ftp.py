"""FTP support"""
from __future__ import absolute_import

import fnmatch
from ftplib import FTP_TLS
import logging
import netrc
import glob
import os
import sys
from typing import (IO, BinaryIO, List,  # pylint: disable=unused-import
                    Text, Union, overload)

from six.moves import urllib
from schema_salad.ref_resolver import uri_file_path

from cwltool.stdfsaccess import StdFsAccess
from cwltool.loghandler import _logger

if sys.version_info < (3, 4):
    from pathlib2 import PosixPath  # pylint: disable=import-error,unused-import
else:
    from pathlib import PosixPath

def abspath(src, basedir):  # type: (Text, Text) -> Text
    if src.startswith(u"file://"):
        ab = Text(uri_file_path(str(src)))
    elif urllib.parse.urlsplit(src).scheme in ['http', 'https', 'ftp']:
        return src
    else:
        if basedir.startswith(u"file://"):
            ab = src if os.path.isabs(src) else basedir+ '/'+ src
        else:
            ab = src if os.path.isabs(src) else os.path.join(basedir, src)
    return ab

class FtpFsAccess(StdFsAccess):
    def __init__(self, basedir):  # type: (Text) -> None
        super(FtpFsAccess, self).__init__(basedir)
        self.cache = {}
        try:
            self.netrc = netrc.netrc()
        except netrc.NetrcParseError as err:
            _logger.warning(err)
            self.netrc = None

    def _connect(self, url):  # type: (Text) -> Optional[FTP]
        parse = parse = urllib.parse.urlparse(url)
        if parse.scheme == 'ftp':
            host = parse.netloc
            user = passwd = ""
            if '@' in parse.netloc:
                (user, host) = parse.netloc.split('@')
            if ':' in user:
                (user, passwd) = user.split(':')
            if not user:
                creds = self.netrc.authenticators(host)
                if creds:
                    user = creds.login
                    passwd = creds.password
            if (host, user, passwd) in self.cache:
                if self.cache[(host, user, passwd)].pwd():
                    sys.stderr.write("FTP cache hit")
                    return self.cache[(host, user, passwd)]
            ftp = FTP_TLS()
            ftp.set_debuglevel(1 if _logger.isEnabledFor(logging.DEBUG) else 0)
            ftp.connect(host)
            ftp.login(user, passwd)
            self.cache[(host, user, passwd)] = ftp
            return ftp
        return None

    def _abs(self, p):  # type: (Text) -> Text
        return abspath(p, self.basedir)

    def glob(self, pattern):  # type: (Text) -> List[Text]
        return self._glob(pattern)

    def _glob0(self, basename, basepath):
        if basename == '':
            if self.isdir(basepath):
                return [basename]
        else:
            if self.isfile(self.join(basepath, basename)):
                return [basename]
        return []

    def _glob1(self, pattern, basepath=None):
        try:
            names = self.listdir(basepath)
        except Exception:
            return []
        if pattern[0] != '.':
            names = filter(lambda x: x[0] != '.', names)
        return fnmatch.filter(names, pattern)

    def _glob(self, pattern, basepath=None):  # type: (Text) -> List[Text]
        if not basepath:
            basepath = self.basedir
        # ftp = self._connect(basepath)
        dirname, basename = pattern.rsplit('/', 1)
        if not glob.has_magic(pattern):
            if basename:
                if not pattern.endswith('/') and \
                        self.isfile(self.join(basepath, pattern)):
                    return [pattern]
            else:  # Patterns ending in slash should match only directories
                if self.isdir(self.join(basepath, dirname)):
                    return [pattern]
            return []
        if not dirname:
            return self._glob1(basename)

        dirs = self._glob(dirname)
        if glob.has_magic(basename):
            glob_in_dir = self._glob1
        else:
            glob_in_dir = self._glob0
        results = []
        for dirname in dirs:
            results.extend(glob_in_dir(dirname, basename))
        return results


    def open(self, fn, mode):
        if 'r' in mode:
            return urllib.request.urlopen(fn)
        raise Exception('Write mode FTP not implemented')

    def exists(self, fn):  # type: (Text) -> bool
        return os.path.exists(self._abs(fn))

    def isfile(self, fn):  # type: (Text) -> bool
        ftp = self._connect(fn)
        if ftp:
            return bool(ftp.size(urllib.parse.urlparse(fn).path))
        return super(FtpFsAccess, self).isfile(fn)

    def isdir(self, fn):  # type: (Text) -> bool
        return bool(self.listdir(fn))

    def listdir(self, fn):  # type: (Text) -> List[Text]
        ftp = self._connect(fn)
        if ftp:
            return ftp.nlst(fn)
        return super(FtpFsAccess, self).listdir(fn)

    def join(self, path, *paths):  # type: (Text, *Text) -> Text
        if path.startswith('ftp'):
            raise Exception('unimplemented')
        return super(FtpFsAccess, self).join(path, *paths)

    def realpath(self, path):  # type: (Text) -> Text
        return os.path.realpath(path)
