"""AWS S3 support"""
from __future__ import absolute_import

import contextlib
import fnmatch
import ftplib
import logging
import netrc
import glob
import boto3
import os
import re
import sys
import exrex
from typing import List, Text  # noqa F401 # pylint: disable=unused-import

from six import PY2
from six.moves import urllib
from schema_salad.ref_resolver import uri_file_path
from typing import Tuple, Optional

from cwltool.stdfsaccess import StdFsAccess
from cwltool.loghandler import _logger
from .ftp import abspath

from .s3file import S3File


class AWSS3Access(StdFsAccess):
    """AWS s3 access with upload."""
    def __init__(self, basedir, cache=None):  # type: (Text) -> None
        super(AWSS3Access, self).__init__(basedir)
        self.cache = cache or {}
        self.uuid=None
        #print("Initializing AWSS3Acces object for basedir {}".format( basedir))

    
    def _parse_url(self, url):
        # type: (Text) -> Tuple[Optional[Text], Optional[Text]]

        # the s3 url can be in one of the following formats:
        #s3://bucket/path/object
        #s3://aws.com/bucket/path/object
        #print("_parse_url: parsing url {}".format(url))
        parse = urllib.parse.urlparse(url)
        # if the parse.netloc contains something like aws...com we will consider this the server and remove it
        if re.search( "aws.+\.com", parse.netloc ):
            bucketM=re.search("(.+?)(/.*)", parse.path)
            bucket=bucketM[1]
            if bucket[0]=='/':
                bucket=bucket[1:]
            
            parse=parse._replace(netloc=bucket)
            parse=parse._replace(path=bucketM[2])
            
            url=urllib.parse.urlunparse(parse)
            parse=self._parse_url( url )
        #user = parse.username
        #passwd = parse.password
        bucket = parse.netloc
        path = parse.path
        if path[0]=='/': path=path[1:]
        #if parse.scheme == 's3':

        #print("_parse_url: bucket {} path {}".format(bucket, path))
        return bucket, path

    def _connect(self, url):  # type: (Text) -> Optional[ftplib.FTP]
        '''caches and returns the s3 connection '''
        parse = urllib.parse.urlparse(url)
        if parse.scheme == 's3':

            bucketname, _ = self._parse_url(url)
            if (bucketname ) in self.cache:
                    return self.cache[(bucketname)]
            session = boto3.session.Session()
            #s3=boto3.resource('s3')
            #bucket=s3.Bucket(  bucketname )
            
            #ftp = ftplib.FTP_TLS()
            #ftp.set_debuglevel(1 if _logger.isEnabledFor(logging.DEBUG) else 0)
            #ftp.connect(host)
            #ftp.login(user, passwd)
            self.cache[(bucketname)] = session
            return session
            #return bucket
        return None

    def setUUID(self, uuid):
        self.uuid=uuid
    def getUUID(self):
        return(self.uuid)

    def _abs(self, p):  # type: (Text) -> Text
        return abspath(p, self.basedir)

    def glob(self, pattern):  # type: (Text) -> List[Text]
        #print("GLOB pattern {}".format(pattern))
        if not self.basedir.startswith("s3:"):
            return super(AWSS3Access, self).glob(pattern)
        return self._glob(pattern)

    def _glob0(self, basename, basepath):
        #print("_GLOB0  basename {} basepath {}".format(basename , basepath))
        if basename == '':
            if self.isdir(basepath):
                return [basename]
        else:
            if self.isfile(self.join(basepath, basename)):
                return [basename]
        return []

    def _glob1(self, pattern, basepath=None):
        #print("_GLOB1  pattern {} basepath {}".format(pattern , basepath))
        try:
            names = self.listdir(basepath)
        except :
            return []
        if pattern[0] != '.':
            names = filter(lambda x: x[0] != '.', names)
        return fnmatch.filter(names, pattern)

    def _glob(self, pattern):  # type: (Text) -> List[Text]
        if pattern.endswith("/."):
            pattern = pattern[:-1]
        dirname, basename = pattern.rsplit('/', 1)
        
        #print("_GLOB dirname {} basename {}".format(dirname, basename))
        if not glob.has_magic(pattern):
            #print("Glbod does not have magic")
            if basename:
                if self.exists(pattern):
                    return [pattern]
            else:  # Patterns ending in slash should match only directories
                if self.isdir(dirname):
                    return [pattern]
            return []
        if not dirname:
            #print("We don't have dirname")
            return self._glob1(basename)

        dirs = self._glob(dirname)
        if glob.has_magic(basename):
            glob_in_dir = self._glob1
        else:
            glob_in_dir = self._glob0
        results = []
        for dirname in dirs:
            results.extend(glob_in_dir(basename, dirname))
        return results
#import exrex
# import boto3
# session = boto3.Session() # profile_name='xyz'
# s3 = session.resource('s3')
# bucket = s3.Bucket('mybucketname')
# 
# prefixes = list(exrex.generate(r'api/v2/responses/2016-11-08/(2016-11-08T2[2-3]|2016-11-09)'))
# 
# objects = []
# for prefix in prefixes:
#     print(prefix, end=" ")
#     current_objects = list(bucket.objects.filter(Prefix=prefix))
#     print(len(current_objects))
#     objects += current_objects
    def open(self, fn, mode):
        
        #print("s3 OPEN for {} {}".format(fn, mode))
        #sys.exit(1)
        
        if not fn.startswith("s3:"):
            return super(AWSS3Access, self).open(fn, mode)
        if 'r' in mode:
            s3session=self._connect(fn)
            bucket, path = self._parse_url(fn)
            s3bucket=s3session.client('s3')
            handle = s3bucket.get_object(Bucket=bucket, Key=path)['Body']
            
            s3 = boto3.resource("s3")
            s3_object = s3.Object(bucket_name=bucket, key=path)
            return S3File( s3_object )
        raise Exception('Write mode s3 not implemented')

    def exists(self, fn):  # type: (Text) -> bool
        #print("EXISTS for file {}".format(fn))
        if not fn.startswith("s3:"):
            return super(AWSS3Access, self).exists(fn)
        return self.isfile(fn) or self.isdir(fn)

    def isfile(self, fn):  # type: (Text) -> bool
        #print("IS FILE for file {}".format(fn))
        s3session = self._connect(fn)
        
        if s3session:
            try:
                sz=self.size(fn)
                if sz is None: 
                    #print("Return False")
                    return False
                #print("Return True")
                return True
            except :
                #print("Return False")
                return False
        return super(AWSS3Access, self).isfile(fn)

    def isdir(self, fn):  # type: (Text) -> bool
        ''' to check if a fn is really a directory
            we list all the objects under this prefix
            It is important not to have a / at teh beginnint of the path'''
        #print("IS DIR for file {}".format(fn))
        s3session = self._connect(fn) # this is a resource for the bucket
        if s3session:
            try:
                (bucketname, path)=self._parse_url( fn )
                if path[0] == '/':
                    path=path[1:]
                if path[-1] != '/':
                    path=path + '/'
                contents=0
                s3bucket=s3session.client('s3')
                for o in s3bucket.list_objects_v2(Bucket=bucketname, Prefix=path)['Contents']:
                    contents=contents+1
                    if contents >0:
                        #print("Return True")
                        return True
                #print("Return False")
                return False
            except:
                #print("Return False")
                return False
        return super(AWSS3Access, self).isdir(fn)

    def mkdir(self, url, recursive=True):
        """Make the directory specified in the URL. For s3 it just creates an ojbect that ends with /"""
        s3session = self._connect(url)
        bucketname,path = self._parse_url(url)
        if path[0] == '/':
            path=path[1:]
        if path[-1] != '/':
            path=path + '/'
        try:
            s3bucket=s3session.client('s3')
            s3bucket.put_object(Bucket=bucketname, Key=path)
            return True
        except:
            return False
        return None

    def listdir(self, fn):  # type: (Text) -> List[Text]
        s3session = self._connect(fn)
        if s3session:
            bucketname, path = self._parse_url(fn)
            #print("s3.py: bucketname {} path {}".format( bucketname, path))
            s3bucket=s3session.client('s3')
            if path[0]=='/':path=path[1:]
            if path[-1]!='/':path=path+'/'
            #print("s3.py: bucketname {} path {}".format( bucketname, path))
            
            return ["s3://{}/{}".format(bucketname, x['Key']) for x in s3bucket.list_objects_v2(Bucket=bucketname, Prefix=path)['Contents']]
        return super(AWSS3Access, self).listdir(fn)

    def join(self, path, *paths):  # type: (Text, *Text) -> Text
        if path.startswith('s3:'):
            result = path
            for extra_path in paths:
                if extra_path.startswith('s3:/'):
                    result = extra_path
                else:
                    if result[-1]=='/': result=result[:-1]
                    result = result + "/" + extra_path
            return result
        return super(AWSS3Access, self).join(path, *paths)

    def realpath(self, path):  # type: (Text) -> Text
        if path.startswith('s3:'):
            return path
        return os.path.realpath(path)

    def size(self, fn):
        s3session = self._connect(fn)
        #raise Exception("size: getting size for {}".format(fn))
        if s3session:
            bucketname, path = self._parse_url(fn)
            
            try:
                s3bucket=s3session.client('s3')
                object=s3bucket.head_object(Bucket=bucketname, Key=path)
                size=object['ContentLength']
                return size
            except :
                return None

        return super(AWSS3Access, self).size(fn)

    def upload(self, file_handle, url):
        """s3 specific method to upload a file to the given URL."""
        s3session = self._connect(url)
        try:
            s3bucket=s3session.client('s3')
            bucketname,path=self._parse_url(url)
            s3bucket.upload_file( Bucket=bucketname, Filename=file_handle, Key=path)
        except:
            raise Exception("Cannot store file {} to {}".format( file_handle, path))
