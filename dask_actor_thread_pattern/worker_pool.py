from queue import Queue
from threading import Event, Thread
import time

from .worker_manager import BaseWorker, WorkerManager

from .utils import ProgressBar

class WorkerThread(Thread):
    
    def __init__(
        self, 
        worker, 
        task_queue, 
        result_queue, 
        idle_event,
        stop_event
    ):
        super(WorkerThread, self).__init__()

        self.worker = worker
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.idle_event = idle_event
        self.stop_event = stop_event

    def run(self):
        
        while True:
            
            is_stateful, method, args, kwargs = self.task_queue.get()
            if is_stateful=="EndOfTasks":
                break
            
            self.idle_event.clear()
            result = self.worker.run(is_stateful, method, *args, **kwargs)
            self.result_queue.put(result)
            self.idle_event.set()
            
        self.stop_event.set()

class Worker(BaseWorker):

    def run(self, is_stateful, method, *args, **kwargs):
        
        if is_stateful:
            return method(self, *args, **kwargs)
        else:
            return method(*args, **kwargs)

class WorkerPool:
    
    def __init__(self, dask_configs):
        
        self.worker_manager = WorkerManager(dask_configs.get_config())
        self.worker_manager.start_workers(Worker)
        self.workers = self.worker_manager.workers
        self.task_queue = Queue()
        self.result_queue = Queue()
        self.worker_threads = dict()
        self.idle_events = dict()
        self.stop_events = dict()
        self.results = []
        self.num_tasks = 0
        
    def start_threads(self):
        
        for worker in self.workers:
            
            idle_event = Event()
            stop_event = Event()
            
            # All workers are idle initially.
            idle_event.set()
            
            t = WorkerThread(
                worker=worker, 
                task_queue=self.task_queue, 
                result_queue=self.result_queue, 
                idle_event=idle_event,
                stop_event=stop_event
            )
            # Make it daemon so that it runs in the background.
            t.setDaemon(True)
            t.start()
            
            self.worker_threads[worker.addr] = t
            self.idle_events[worker.addr] = idle_event
            self.stop_events[worker.addr] = stop_event
            
    def submit(self, method, *args, **kwargs):
        
        if self._all_threads_stopped():
            self.start_threads()
        
        self.task_queue.put([False, method, args, kwargs])
        
        self.num_tasks += 1
        
    def submit_stateful(self, method, *args, **kwargs):
        
        if self._all_threads_stopped():
            self.start_threads()
        
        self.task_queue.put([True, method, args, kwargs])
        
        self.num_tasks += 1
        
    def submit_foreach(self, method, *args, **kwargs):
        
        if not self._all_threads_stopped():
            raise ValueError("Please invoke submit_foreach when workers are done working.")
        
        results = []
        
        for worker in self.workers:
            
            result = worker.run(True, method, *args, **kwargs)
            results.append(result)
            
        return results
        
    def get(self):
        
        pb = ProgressBar(self.num_tasks)
        
        if self._all_threads_stopped():
            return self._close()
        
        while True:
            
            queue_empty = self.task_queue.empty()
            all_idle = self._all_workers_idle()
            
            if queue_empty and all_idle:
                break
        
            time.sleep(0.5)
            pb.report(self.result_queue.qsize())
            
        while True:
            
            self.task_queue.put(["EndOfTasks", None, None, None])
            
            if self._all_threads_stopped():
                break
                
            time.sleep(0.1)
            
        return self._close()
    
    def _close(self):
        
        self.num_tasks = 0
        self.task_queue.queue.clear()
        
        while not self.result_queue.empty():
            self.results.append(self.result_queue.get())
            
        return self.results
        
    def _all_workers_idle(self):
        
        return all([idle_event.isSet() for idle_event in self.idle_events.values()])
    
    def _all_threads_stopped(self):
        
        return all([stop_event.isSet() for stop_event in self.stop_events.values()])

    def scatter_data(self, target_dir, s3_url, object_name):

    	return self.worker_manager.scatter_data(target_dir, s3_url, object_name)

    def close(self):

    	self.worker_manager.close()
