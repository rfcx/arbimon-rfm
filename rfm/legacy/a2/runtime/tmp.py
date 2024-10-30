"""
Utility module for temp files.
"""

import os
import tempfile

class tmpfile(object):
    """
    A tempfile tracker object.
    provides usefull enter, close context handlers to automatically 
    create and remove a tempfile using mkstemp() from the builtin tempfile module.
    Cosntructor arguments are given to mkstemp() upon entering the context
    and self is returned. The tempfile is removed (os.remove()) upon exiting the context.
    The tempfile's file handle can be closed mid-way with the close_file() method.
    """
    def __init__(self, *args, **kwargs):
        "Initialize the tracker object, arguments are given to tempfile.mkstemp()"
        self.args = args
        self.kwargs = kwargs
        self.file=None
        self.filename=None
        
    def close_file(self):
        "Closes the tempfile's file handle"
        self.file.close()
        self.file = None
        
    def __enter__(self):
        "Creates a tempfile upon entering the context"
        file_handle, self.filename = tempfile.mkstemp(*self.args, **self.kwargs)
        self.file = os.fdopen(file_handle, 'w')
        return self
        
    def __exit__(self, xtype, value, traceback):
        "Removes the tempfile upon exiting the context"
        if self.file:
            self.file.close()
        
        os.remove(self.filename)