"""
    Module for tagging objects with decorators.
    Each tag has a tag_type and a value.
    Tagged things can then be obtained through get().
"""

__tags = {}

def get(tag_type, value):
    "returns a tagged object given the tagged type and value."
    return __tags.get(tag_type, {}).get(value)
    
def tag(tag_type, value):
    "tag decorator"
    def tag_decorator(obj):
        obj.tag = value
        if tag_type not in __tags:
            __tags[tag_type] = {}
        __tags[tag_type][value] = obj
        return obj
        
    return tag_decorator
