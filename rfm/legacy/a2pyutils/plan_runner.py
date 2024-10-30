import multiprocessing
# import pathos.multiprocessing
from joblib import Parallel, delayed
import dill


class PlanRunner(object):
    def __init__(self, 
        plan, 
        logger, mark_progress, check_canceled,
        num_cores=None 
    ):
        self.plan = plan
        self.logger = logger
        self.mark_progress = mark_progress
        self.check_canceled = check_canceled
        self.num_cores = num_cores or multiprocessing.cpu_count()

    def execute(self):
        last_result = None
        
        for step in self.plan:
            self.logger.write(step.name)
            if step.parallelizable:
                last_result = self.run_parallel_step(step)
            else :
                last_result = self.run_step(step)
            step.result = last_result
            self.mark_progress(step.progress)
            if self.check_canceled():
                print 'job canceled!'
                break
        return last_result
    
    def compute_step_inputs(self, step):
        inputs = {}
        
        for k,v in step.inputs:
            if isinstance(k, int):
                prev_step = self.plan.steps[step.index + k]
                k = prev_step.name
                v = prev_step.result
                
            inputs[k] = v
        
        for step in self.plan:
            if step.name in inputs:
                inputs[step.name] = step.result
                
        return inputs


    def run_step(self, step):
        inputs = self.compute_step_inputs(step)
        return step.fn(step, inputs)
        
    def run_parallel_step(self, step):
        inputs = self.compute_step_inputs(step)
        substeps = [
            delayed(run_parallel_substep)(dumps([
                subindex, item, 
                step, 
                inputs,
                self.mark_progress, self.check_canceled
            ])) 
            for subindex, item in enumerate(step.data or [])        
        ]
        
        return Parallel(n_jobs=self.num_cores)(substeps)

def dumps(object):
    return dill.dumps(object).encode('zlib')
    
def loads(state):
    return dill.loads(state.decode('zlib'))

        
def run_parallel_substep(state):
    subindex, item, step, inputs, mark_progress, check_canceled = loads(state)

    if check_canceled():
        return "canceled"

    result = step.fn(subindex, item, step, inputs)
    mark_progress(step.cost, relative=True)
    
    return result
        
