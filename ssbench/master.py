import yaml

from ssbench.constants import *

class Master:
    def __init__(self, queue):
        self.queue = queue
        
    def go(self):
        self.queue.put(yaml.dump({"type": UPLOAD_OBJECT}))
    
