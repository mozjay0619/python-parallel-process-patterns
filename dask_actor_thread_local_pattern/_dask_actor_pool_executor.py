import time
from queue import Queue
from threading import Event, Thread

from .utils import ProgressBar


class PoolWorkerThread(Thread):
    def __init__(self, worker, task_queue, result_queue, idle_event, stop_event):
        super(PoolWorkerThread, self).__init__()

        self.worker = worker
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.idle_event = idle_event
        self.stop_event = stop_event

    def run(self):

        while True:

            key, method_attrs, method, args, kwargs = self.task_queue.get()
            if method_attrs[0] == "ENDOFTASKS":
                break

            self.idle_event.clear()
            if method_attrs[0] == "STATEFUL":
                result = self.worker.run_stateful_method(method, *args, **kwargs)
            else:
                result = self.worker.run_method(method, *args, **kwargs)
            self.result_queue.put((key, result))
            self.idle_event.set()

        self.stop_event.set()


class DaskActorPoolExecutor:
    """This backend uses Dask distributed Actors instead of native Python multiprocessing.
    Advantage:
    1. Stateful process using Dask's Actor class.
    2. Can use on cluster.
    Disadvantage:
    1. No Unix copy-on-write, and hence no apply methods.
    2. A little slow to initialize the processes.
    List of available (O) and unavailable (X) methods:
    1. apply_to (X)
    2. apply, apply_keyed (X)
    3. capply, capply_keyed (X)
    4. gapply, gapply_keyed (X)
    5. submit, submit_keyed (O)
    6. submit_foreach (O)
    7. submit_stateful, submit_stateful_keyed (O)
    """

    def __init__(self, worker_manager=None, workers=None):

        if worker_manager is not None:
            self.worker_manager = worker_manager
            self.workers = self.worker_manager.workers
        if workers is not None:
            self.worker_manager = None
            self.workers = workers

        self.task_queue = Queue()
        self.result_queue = Queue()
        self.worker_threads = dict()
        self.idle_events = dict()
        self.stop_events = dict()
        self.results = dict()
        self.num_tasks = 0
        self.data_io = None

    def start_threads(self):
        """Start the daemon threads where the Dask process is called upon
        to grab tasks from the task queue forever in an infinite for loop.
        Refer to docstrings for PoolWorkerThread.
        """
        if not self._all_threads_stopped():
            return

        # Open one daemon thread per Dask worker.
        for worker in self.workers:

            idle_event = Event()
            stop_event = Event()

            # All workers are idle initially.
            idle_event.set()

            t = PoolWorkerThread(
                worker=worker,
                task_queue=self.task_queue,
                result_queue=self.result_queue,
                idle_event=idle_event,
                stop_event=stop_event,
            )
            # Make it daemon so that it runs in the background.
            t.setDaemon(True)
            t.start()

            self.worker_threads[worker.__addr__] = t
            self.idle_events[worker.__addr__] = idle_event
            self.stop_events[worker.__addr__] = stop_event

    @staticmethod
    def _submit_method(key, method_attrs, method, *args, **kwargs):
        """The entry method for the parallelization for submitted methods.
        Parameters
        ----------
        method_attrs : list
            The list of attributes associated with the task method.
        method : Python method
            The user method.
        args, kwargs
            The arguments for the user method.
        """
        return key, method_attrs, method(*args, **kwargs)

    def _submit(self, key, method_attrs, method, *args, **kwargs):
        """Utility method for task submission for all the submit and submit_stateful
        methods. (Not for submit_foreach)
        This method puts the task on the task queue that the available worker
        grabs the next task from.
        """
        self.start_threads()
        task = [key, method_attrs, method, args, kwargs]
        self.task_queue.put(task)

    def submit(self, method, *args, **kwargs):
        """Invoke this method to submit user methods and arguments. This method
        is used for stateless method.
        Parameters
        ----------
        method : Python method
            The user method.
        args, kwargs
            The arguments for the user method.
        """
        self.num_tasks += 1
        self._submit(self.num_tasks, ["STATELESS"], method, *args, **kwargs)

    def submit_keyed(self, key, method, *args, **kwargs):
        """Same as submit, but it allows users to specify the custom
        key for this task.
        Parameters
        ----------
        key : str
            The key associated with this task.
        method : Python method
            The user method.
        args, kwargs
            The arguments for the user method.
        """
        self.num_tasks += 1
        self._submit(key, ["STATELESS"], method, *args, **kwargs)

    def submit_stateful(self, method, *args, **kwargs):
        """Invoke this method to submit user methods and arguments. This method
        is used for stateful method. This means that the first parameter of
        the method must be "self", which refers to the Dask process object.
        Parameters
        ----------
        method : Python method
            The user method.
        args, kwargs
            The arguments for the user method.
        """
        self.num_tasks += 1
        self._submit(self.num_tasks, ["STATEFUL"], method, *args, **kwargs)

    def submit_stateful_keyed(self, key, method, *args, **kwargs):
        """Same as submit_stateful, but it allows users to specify the custom
        key for this task.
        Parameters
        ----------
        key : str
            The key associated with this task.
        method : Python method
            The user method.
        args, kwargs
            The arguments for the user method.
        """
        self.num_tasks += 1
        self._submit(key, ["STATEFUL"], method, *args, **kwargs)

    def submit_foreach(self, method, *args, **kwargs):
        """Submit the method to each worker just once. This is useful for when
        you want to update the state of each worker.
        Parameters
        ----------
        method : Python method
            The user method.
        args, kwargs
            The arguments for the user method.
        """
        if not self._all_threads_stopped():
            raise ValueError(
                "Please invoke submit_foreach when workers are done working."
            )

        futures = []
        for worker in self.workers:
            futures.append(worker.run_stateful_method_future(method, *args, **kwargs))

        pb = ProgressBar(len(self.workers))

        while any([future.q.empty() for future in futures]):
            pb.report(len([future for future in futures if not future.q.empty()]))
            time.sleep(0.5)

        pb.report(len([future for future in futures if not future.q.empty()]))

        results = [future.result() for future in futures]
        return results

    def get(self, key=None, defaults=None):
        """Invoke this method to aggregate the results of the parallel
        computations. If nothing is passed to this method, all of the results
        will be returnd in a list.
        Also, this method will bring all the worker threads to their termination.
        The workers themselves are still online. They can only be closed by invoking
        close method.
        Parameters
        ----------
        key : str
            If provided, get will return the result of the task that is associated
            with this key.
        defaults : Python object
            The default return values in case the return value is not available
            from the computations.
        """
        pb = ProgressBar(self.num_tasks)

        if self._all_threads_stopped():
            return self._postprocess(key, defaults)

        while True:

            queue_empty = self.task_queue.empty()
            all_idle = self._all_workers_idle()

            if queue_empty and all_idle:
                break

            time.sleep(0.5)
            pb.report(self.result_queue.qsize())

        while True:

            self.task_queue.put([None, ["ENDOFTASKS"], None, None, None])

            if self._all_threads_stopped():
                break

            time.sleep(0.1)

        return self._postprocess(key, defaults)

    def _postprocess(self, key, defaults):

        self.num_tasks = 0
        self.task_queue.queue.clear()

        while not self.result_queue.empty():

            _key, result = self.result_queue.get()

            self.results[_key] = result

        if not key:
            return list(self.results.values())
        else:
            return self.results.get(key, defaults)

    def _all_workers_idle(self):
        """Returns True if all workers are idle."""
        return all([idle_event.isSet() for idle_event in self.idle_events.values()])

    def _all_threads_stopped(self):
        """Returns True if all workers are stopped. This means the infinite
        while loop of the worker threads are all broken.
        """
        return all([stop_event.isSet() for stop_event in self.stop_events.values()])

    def close(self):
        """Call closure on workers through the worker manager. This means
        the inifinite while loop is still being run but the workers are
        idling due to empty task queue.
        """
        self.worker_manager.close()

    def clear(self):
        """Clear out the results from past task submissions."""
        self.num_tasks = 0
        self.results = dict()