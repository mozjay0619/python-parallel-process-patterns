import ray 
import numpy as np 
import pandas as pd  
import time

from .actor_pool_extended import ActorPoolExtended
from .utils import ProgressBar

class BaseWorker:
    
    def __init__(self):
        
        pass
        
    def run(self, key, method, *args, **kwargs):
        
        return key, method(self, *args, **kwargs)

class Futures():
    
    def __init__(self, num_procs=4, worker=None, *args, **kwargs):
        
        self.pending_tasks = {}
        self.results = {}
        self.mode = 'stateless'
        
        if worker is not None:
            
            RemoteWorker = ray.remote(worker)

            worker_actors = []
            
            for i in range(num_procs):
                worker_actor = RemoteWorker.remote(*args, **kwargs)
                worker_actors.append(worker_actor)
                
            self.pool = ActorPoolExtended(worker_actors)
            
            self.mode = 'stateful'
            

    def apply_to(self, dataframe):
        
        self.dataframe = dataframe
        self.dataframe_ref = ray.put(dataframe)
        
    def capply(self, method, *args, **kwargs):
        
        self.capply_keyed(len(self.pending_tasks), method, *args, **kwargs)
        
    def capply_keyed(self, key, method, *args, **kwargs):
        
        remote_method = ray.remote(method)
        remote_ref = remote_method.remote(self.dataframe_ref, *args, **kwargs)
        
        self.pending_tasks[remote_ref] = (key, 'capply_type')
        
    def apply(self, method, *args, **kwargs):
        
        self.apply_keyed(len(self.pending_tasks), method, *args, **kwargs)
        
    def apply_keyed(self, key, method, *args, **kwargs):
        
        remote_method = ray.remote(method)
        remote_ref = remote_method.remote(self.dataframe_ref, *args, **kwargs)
        
        self.pending_tasks[remote_ref] = (key, 'apply_type')
        
    def submit(self, method, *args, **kwargs):
        
        self.submit_keyed(len(self.pending_tasks), method, *args, **kwargs)
    
    def submit_keyed(self, key, method, *args, **kwargs):
        
        remote_method = ray.remote(method)
        remote_ref = remote_method.remote(*args, **kwargs)
        
        self.pending_tasks[remote_ref] = (key, 'submit_type')
        
    def submit_stateful(self, method, *args, **kwargs):
        
        self.submit_stateful_keyed(len(self.pending_tasks), method, *args, **kwargs)
        
    def submit_stateful_keyed(self, key, method, *args, **kwargs):
        
        self.pool.submit(key, method, *args, **kwargs)
        
        self.pending_tasks[key] = (key, 'stateful_type')
        
        
    def get(self):
        
        bar = ProgressBar(len(self.pending_tasks))
        
        if self.mode=='stateless':
        
            remote_refs = list(self.pending_tasks)

            while len(remote_refs):

                completed_task_ref, remote_refs = ray.wait(remote_refs)

                key, method_type = self.pending_tasks.pop(completed_task_ref[0], None)

                result = ray.get(completed_task_ref[0])

                self.process_completed_task(key, method_type, result)

                bar.report(len(self.results))
    
        elif self.mode=='stateful':
            
            while len(futures.pending_tasks):

                key, result = futures.pool.get_next_unordered()
                
                futures.pending_tasks.pop(key)
                
                self.results[key] = result
                
                bar.report(len(self.results))
            
        bar.report(len(self.pending_tasks))
        
    def process_completed_task(self, key, method_type, result):
        
        is_list = isinstance(result, list)
        
        if not is_list:
            result = [result]
        
        if method_type=='capply_type':
            
            for i, elem in enumerate(result):

                if isinstance(elem, pd.DataFrame):
                    
                    if len(elem) != len(self.dataframe):
                        continue
                    
                    if isinstance(key, list):
                        elem.columns = key

                elif isinstance(elem, pd.Series):
                    
                    if len(elem) != len(self.dataframe):
                        continue
                    
                    if isinstance(key, list):
                        elem.name = key[0]
                        
                elif isinstance(elem, np.ndarray):
                    
                    if len(elem) != len(self.dataframe):
                        continue
                    
                    if len(elem.shape)==1:
                        elem = pd.Series(elem)
                        
                        if isinstance(key, list):
                            elem.name = key[0]
                            
                    elif len(elem.shape)==2:
                        elem = pd.DataFrame(elem)
                        
                        if isinstance(key, list):
                            elem.columns = key
                        
                    else:
                        continue
                        
                else:
                    continue

                self.dataframe = self.dataframe.join(elem)
                result[i] = None

        if is_list:
            self.results[key] = result
        else:
            self.results[key] = result[0]
