
class Plan(object):
    def __init__(self, *args):
        self.steps = map(Step.cast, args)
    
    def insert(self, pos, *args):
        self.steps[pos:pos] = map(Step.cast, args)
    
    def append(self, *args):
        self.insert(len(self.steps), *args)
    
    def prepend(self, *args):
        self.insert(0, *args)
    
    def compute_plan_cost(self):
        cost = 0
        for index, step in enumerate(self.steps):
            step.index = index
            cost += step.steps * step.cost
            step.progress = cost
        self.cost = cost
        return cost
        
    def __repr__(self):
        return "{}({})".format(
            self.__class__.__name__, ", ".join(map(repr, self.steps))
        )
        
    def __iter__(self):
        return iter(self.steps)


class Step(object):
    def __init__(self, name, fn, steps=1, cost=1, parallelizable=False, data=None, inputs=None):
        self.name = name
        self.fn = fn
        self.steps = steps
        self.parallelizable = parallelizable
        self.data = data
        self.inputs = inputs or []
        self.cost = cost
        self.progress = 0
    
    @classmethod
    def cast(cls, obj):
        if isinstance(obj, Step):
            return obj
        else:
            return Step(**obj)

    def __repr__(self):
        return "{}({})".format(
            self.__class__.__name__, ", ".join(x for x in [
                repr(self.name),
                self.steps != 1 and "steps={}".format(self.steps),
                self.cost != 1 and "cost={}".format(self.cost),
                self.parallelizable and "parallelizable={}".format(self.parallelizable)
            ] if x)
        )
