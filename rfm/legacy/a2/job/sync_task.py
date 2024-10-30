import tasks
import a2.runtime as runtime


@runtime.tags.tag('task_type', 'job.sync')
class SyncTask(tasks.Task):
    "Empty no-op task"
    def run(self):
        print "syncing tasks :-)"

