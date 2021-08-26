import urllib
import os
import cwltool.argparser
import json
from typing import (
    AnyStr,
    cast
)
from schema_salad.ref_resolver import file_uri



class BucketFetcher(DefaultFetcher):
    def __init__(self, cache, session, api_client=None, fs_access=None, num_retries=4):
        super(CollectionFetcher, self).__init__(cache, session)
        self.api_client = api_client
        self.fsaccess = fs_access
        self.num_retries = num_retries

    def fetch_text(self, url, content_types=None):
        if url.startswith("s3:"):
            with self.fsaccess.open(url, "r", encoding="utf-8") as f:
                return f.read()
        
        return super(CollectionFetcher, self).fetch_text(url)

    def check_exists(self, url):
        try:

            if url.startswith("s3:"):
                return self.fsaccess.exists(url)
            if url.startswith("arvwf:"):
        except Exception:
            logger.exception("Got unexpected exception checking if file exists")
            return False
        return super(CollectionFetcher, self).check_exists(url)

    def urljoin(self, base_url, url):
        if not url:
            return base_url

        urlsp = urllib.parse.urlsplit(url)
        if urlsp.scheme or not base_url:
            return url

        basesp = urllib.parse.urlsplit(base_url)
        if basesp.scheme == "s3":
            if not basesp.path:
                raise IOError(errno.EINVAL, "Invalid s3 path", base_url)

            baseparts = basesp.path.split("/")
            urlparts = urlsp.path.split("/") if urlsp.path else []

            locator = baseparts.pop(0)


# TODO check if we have a well formatted s3 object (with scheme, bucket and path)
            if (basesp.scheme == "s3" and
                (not arvados.util.keep_locator_pattern.match(locator)) and
                (not arvados.util.collection_uuid_pattern.match(locator))):
                raise IOError(errno.EINVAL, "Invalid Keep locator", base_url)

            if urlsp.path.startswith("/"):
                baseparts = []
                urlparts.pop(0)

            if baseparts and urlsp.path:
                baseparts.pop()

            path = "/".join([locator] + baseparts + urlparts)
            return urllib.parse.urlunsplit((basesp.scheme, "", path, "", urlsp.fragment))

        return super(CollectionFetcher, self).urljoin(base_url, url)

    schemes = [u"file", u"s3" ]

    def supported_schemes(self):  # type: () -> List[Text]
        return self.schemes