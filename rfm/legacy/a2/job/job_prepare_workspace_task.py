import os
import os.path
import shutil
import tasks
import a2.runtime as runtime

def makedirs(path):
    "mkdir -p $path"
    try: 
        os.makedirs(path)
    except StandardError:
        if not os.path.isdir(path):
            raise

@runtime.tags.tag('task_type', 'job.prepare_workspace')
class JobPrepareWorkspaceTask(tasks.Task):
    "Task that prepares a workspace in the shared FS"
    def run(self):
        workspace_path = self.get_workspace_path()
        makedirs(workspace_path)
        
        for folder in self.get_args():
            makedirs(os.path.join(workspace_path, folder))