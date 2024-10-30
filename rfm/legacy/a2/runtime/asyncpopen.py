"""
Module that manages subprocess opening in an asynchronous manner.
"""

import threading
import subprocess
import traceback
import time


process_list=[]

more_processes_available_event = threading.Event()
running_thread = None

def popen(args, callback):
    process_list.append((subprocess.Popen(args), callback))
    init()
    more_processes_available_event.set()

def init():
    "Called to initialize the asyncpopen poll thread."
    global running_thread, more_processes_available_event
    
    if running_thread:
        return running_thread

    def worker():
        "demonic thread worker function"
        while True: # loop forever
            # wait for processes to appear
            more_processes_available_event.wait()
            # loop while there is a list of processes
            while len(process_list) > 0:
                for entry in process_list:
                    proc, callback = entry
                    retcode = proc.poll()
                    if retcode is not None: # Process finished.
                        process_list.remove(entry)
                        try:
                            callback(retcode)
                        except Exception:
                            print "Exception while calling process callback"
                            traceback.print_exc()
                time.sleep(.5)
            # indicate that processes have all gone
            more_processes_available_event.clear()

    running_thread = threading.Thread(target=worker)
    running_thread.daemon=True
    running_thread.start()
    
    return running_thread
