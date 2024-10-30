def self_noargs(func):
    "Memoizes a noarg function"

    def memoized_fn(self):
        "memoized function"
        if not hasattr(memoized_fn, 'val'):
            memoized_fn.val = func(self)
        return memoized_fn.val

    memoized_fn.__name__ = func.__name__
    memoized_fn.__doc__ = func.__doc__

    return memoized_fn

def noargs(func):
    "Memoizes a noarg function"

    def memoized_fn():
        "memoized function"
        if not hasattr(memoized_fn, 'val'):
            memoized_fn.val = func()
        return memoized_fn.val

    memoized_fn.__name__ = func.__name__
    memoized_fn.__doc__ = func.__doc__

    return memoized_fn


def hashed(func):
    "Memoizes a function by hashing the arguments (arguments must be hashable)."

    def memoized_fn(*args, **kwargs):
        "memoized function"
        key = (args, tuple(kwargs.items()))
        if key not in memoized_fn.vals:
            memoized_fn.vals[key] = func(*args, **kwargs)
        return memoized_fn.vals[key]

    memoized_fn.__name__ = func.__name__
    memoized_fn.__doc__ = func.__doc__
    memoized_fn.vals = {}

    return memoized_fn
