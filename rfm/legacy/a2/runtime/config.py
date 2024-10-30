import a2pyutils.config

__config__ = None

def get_config():
    global __config__
    if not __config__:
        __config__ = a2pyutils.config.EnvironmentConfig()
    return __config__
