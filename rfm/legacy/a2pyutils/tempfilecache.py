import os
import time
import re
import os.path
from hashlib import sha256
from a2pyutils.config import EnvironmentConfig


class CacheMiss(object):
    def __init__(self, cache, key):
        self.cache = cache
        self.key = key
        self.file = self.cache.key2File(self.key)

    def set_file_data(self, data):
        self.cache.put(self.key, data)

    def retry_get(self):
        return self.cache.get(self.key)


class Cache(object):
    @staticmethod
    def hash_key(key):
        matchRE = re.compile('^(.*?)((\.[^.\/]*)*)?$')
        match = matchRE.match(key)
        return sha256(match.group(1)).hexdigest() + (match.group(2) or '')

    def __init__(self, root=None, config=None):
        if not root:
            config = config if config else EnvironmentConfig()
            root = os.path.realpath(config.tmpfilecacheConfig['path'])
        self.root = root

    def key2File(self, key):
        return os.path.join(self.root, self.hash_key(key))

    def checkValidity(self, file):
        try:
            stats = os.stat(file)
            return {"path": file, "stat": stats}
        except OSError:
            return None

    def get(self, key):
        return self.checkValidity(self.key2File(key))

    def put(self, key, data):
        file = self.key2File(key)
        with open(file, 'wb') as fout:
            fout.write(data)

    def fetch(self, key):
        entry = self.get(key)
        if entry:
            return entry
        else:
            return CacheMiss(self, key)
