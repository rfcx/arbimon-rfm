#!/usr/bin/env python
import json
import os
import os.path

DEFAULT_CONFIG_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'config'))

class AbstractConfig(object):
    def __init__(self):
        self.cache = {}

    def data(self):
        return [
            self.dbConfig['host'],
            self.dbConfig['user'],
            self.dbConfig['password'],
            self.dbConfig['database'],
            self.awsConfig['bucket_name'],
            self.awsConfig['access_key_id'],
            self.awsConfig['secret_access_key'],
            self.rfcxawsConfig['bucket_name'],
            self.rfcxawsConfig['access_key_id'],
            self.rfcxawsConfig['secret_access_key']
        ]

    def __getattr__(self, identifier):
        cfg, magic = identifier[:-6], identifier[-6:]
        if cfg and magic == "Config":
            if cfg not in self.cache:
                return self.load(cfg)
            else:
                return self.cache[cfg]

    def load(self, cfg):
        config = {}
        self.cache[cfg] = config
        return config

class CachedConfig(AbstractConfig):
    def __init__(self, cache):
        self.cache = cache

class EnvironmentConfig(AbstractConfig):
    scache = {}

    def __init__(self, env=None, env_file=None, sample_file=None):
        super(EnvironmentConfig, self).__init__()
        
        self.__initially_empty_env = not env
        if not env:
            env = os.environ

        self.env_file = env_file if env_file else os.path.join(DEFAULT_CONFIG_FOLDER, 'config.env')
        self.sample_file = sample_file if sample_file else os.path.join(DEFAULT_CONFIG_FOLDER, 'config.env.sample')

        if os.path.isfile(self.env_file):
            self.read_env_file(env)
        
        self.check_against_sample_file(env)
        self.parse_env(env)
        
    def read_env_file(self, env, env_file=None):
        env_file = env_file if env_file else self.env_file
        if env_file:
            with open(env_file) as finp:
                for line in (x.strip() for x in finp):
                    if not line or line[0] == '#':
                        continue
                    attr, val = line.split("=", 1)
                    if attr not in env:
                        if val and val[0]+val[-1] in ('""', "''"):
                            val = val[1:-1]
                        env[attr] = val
        return env
        
    def check_against_sample_file(self, env):
        if self.sample_file:
            sample_env = self.read_env_file({}, self.sample_file)
            missing = [attr for attr in sample_env.keys() if attr not in env]
            
            if missing:
                raise StandardError("Environment variables missing: " + ", ".join(missing))
        
        
    def parse_env(self, env):
        for attr, val in env.items():
            comps = attr.lower().split('__')
            attr = comps.pop()
            
            node = self.cache
            while comps:
                comp = comps.pop(0)
                if comp not in node:
                    node[comp] = {}
                node = node[comp]
            node[attr] = val


import dill
def unpickle_EnvironmentConfig(files, cache=None):
    return EnvironmentConfig(None, *files) if files else CachedConfig(cache)
    
@dill.register(EnvironmentConfig)
def pickle_EnvironmentConfig(pickler, config):
    pickler.save_reduce(unpickle_EnvironmentConfig, (
        [config.env_file, config.sample_file], 
    ) if config.__initially_empty_env else (
        False,
        config.cache, 
    ), obj=config)
