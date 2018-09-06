"""FTP support"""
from __future__ import absolute_import

import fnmatch
import ftplib
import logging
import netrc
import glob
import os
from typing import List, Text  # noqa F401 # pylint: disable=unused-import

from six.moves import urllib
from schema_salad.ref_resolver import uri_file_path

from cwltool.stdfsaccess import StdFsAccess
from cwltool.loghandler import _logger


def abspath(src, basedir):  # type: (Text, Text) -> Text
    """http(s):, file:, ftp:, and plain path aware absolute path"""
    if src.startswith(u"file://"):
        apath = Text(uri_file_path(str(src)))
    elif urllib.parse.urlsplit(src).scheme in ['http', 'https', 'ftp']:
        return src
    else:
        if basedir.startswith(u"file://"):
            apath = src if os.path.isabs(src) else basedir + '/' + src
        else:
            apath = src if os.path.isabs(src) else os.path.join(basedir, src)
    return apath


class FtpFsAccess(StdFsAccess):
    """Basic FTP access."""
    def __init__(self, basedir, cache=None):  # type: (Text) -> None
        super(FtpFsAccess, self).__init__(basedir)
        self.cache = cache or {}
        self.netrc = None
        try:
            if 'HOME' in os.environ:
                if os.path.exists(os.path.join(os.environ['HOME'], '.netrc')):
                    self.netrc = netrc.netrc(
                        os.path.join(os.environ['HOME'], '.netrc'))
            elif os.path.exists(os.path.join(os.curdir, '.netrc')):
                self.netrc = netrc.netrc(os.path.join(os.curdir, '.netrc'))
        except netrc.NetrcParseError as err:
            _logger.debug(err)

    def _parse_url(self, url):
        # type: (Text) -> Tuple[Optional[Text], Optional[Text]]
        parse = urllib.parse.urlparse(url)
        user = parse.username
        passwd = parse.password
        host = parse.netloc
        path = parse.path
        if parse.scheme == 'ftp':
            if not user and self.netrc:
                creds = self.netrc.authenticators(host)
                if creds:
                    user, _, passwd = creds
        if not user:
            user = "anonymous"
            passwd = "anonymous@"
        return host, user, passwd, path

    def _connect(self, url):  # type: (Text) -> Optional[ftplib.FTP]
        parse = urllib.parse.urlparse(url)
        if parse.scheme == 'ftp':
            host, user, passwd, _ = self._parse_url(url)
            if (host, user, passwd) in self.cache:
                if self.cache[(host, user, passwd)].pwd():
                    return self.cache[(host, user, passwd)]
            ftp = ftplib.FTP_TLS()
            ftp.set_debuglevel(1 if _logger.isEnabledFor(logging.DEBUG) else 0)
            ftp.connect(host)
            ftp.login(user, passwd)
            self.cache[(host, user, passwd)] = ftp
            return ftp
        return None

    def _abs(self, p):  # type: (Text) -> Text
        return abspath(p, self.basedir)

    def glob(self, pattern):  # type: (Text) -> List[Text]
        if not self.basedir.startswith("ftp:"):
            return super(FtpFsAccess, self).glob(pattern)
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
        except ftplib.all_errors:
            return []
        if pattern[0] != '.':
            names = filter(lambda x: x[0] != '.', names)
        return fnmatch.filter(names, pattern)

    def _glob(self, pattern):  # type: (Text) -> List[Text]
        dirname, basename = pattern.rsplit('/', 1)
        if not glob.has_magic(pattern):
            if basename:
                if self.exists(pattern):
                    return [pattern]
            else:  # Patterns ending in slash should match only directories
                if self.isdir(dirname):
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
        if not self.basedir.startswith("ftp:"):
            return super(FtpFsAccess, self).open(fn, mode)
        if 'r' in mode:
            host, user, passwd, path = self._parse_url(fn)
            return urllib.request.urlopen(
                "ftp://{}:{}@{}/{}".format(user, passwd, host, path))
        raise Exception('Write mode FTP not implemented')

    def exists(self, fn):  # type: (Text) -> bool
        if not self.basedir.startswith("ftp:"):
            return super(FtpFsAccess, self).exists(fn)
        return self.isfile(fn) or self.isdir(fn)

    def isfile(self, fn):  # type: (Text) -> bool
        ftp = self._connect(fn)
        if ftp:
            try:
                ftp.size(urllib.parse.urlparse(fn).path)
                return True
            except ftplib.all_errors:
                return False
        return super(FtpFsAccess, self).isfile(fn)

    def isdir(self, fn):  # type: (Text) -> bool
        ftp = self._connect(fn)
        if ftp:
            try:
                cwd = ftp.pwd()
                ftp.cwd(urllib.parse.urlparse(fn).path)
                ftp.cwd(cwd)
                return True
            except ftplib.all_errors:
                return False
        return super(FtpFsAccess, self).isdir(fn)

    def mkdir(self, url, recursive=True):
        """Make the directory specified in the URL."""
        ftp = self._connect(url)
        path = urllib.parse.urlparse(url).path
        if not recursive:
            return ftp.mkd(path)
        dirs = [d for d in path.split('/') if d != '']
        for index, _ in enumerate(dirs):
            try:
                ftp.mkd("/".join(dirs[:index+1])+'/')
            except ftplib.all_errors:
                pass
        return None

    def listdir(self, fn):  # type: (Text) -> List[Text]
        ftp = self._connect(fn)
        if ftp:
            return ftp.nlst(fn)
        return super(FtpFsAccess, self).listdir(fn)

    def join(self, path, *paths):  # type: (Text, *Text) -> Text
        if path.startswith('ftp:'):
            if paths:
                return path+'/'+'/'.join(paths)
            return path
        return super(FtpFsAccess, self).join(path, *paths)

    def realpath(self, path):  # type: (Text) -> Text
        if path.startswith('ftp:'):
            return path
        return os.path.realpath(path)
