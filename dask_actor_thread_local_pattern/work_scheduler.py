import random
import time
from queue import Queue
from threading import Event, Thread

from .utils import ProgressBar


class SchedulerWorkerThread(Thread):
    """Each worker will be placed in its own thread. The Dask
    Actor is still not developed enough to support functionalities
    other than getting results back from the individual futures.
    While the worker is busy working in the different processes,
    until that work is communicated back to the current process,
    this waiting is as good as I/O bound from the standpoint current
    process. Threads are good tools to use for concurrently dealing
    with many I/O bound tasks.
    """

    def __init__(
        self, worker, task_queue, result_queue, idle_worker_queue, work_done_event
    ):
        super(SchedulerWorkerThread, self).__init__()

        self.worker = worker
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.idle_worker_queue = idle_worker_queue
        self.work_done_event = work_done_event

    def run(self):

        # While the queue of tasks that this worker was assigned
        # to is not empty, keep consuming tasks from the queue
        # and work it, and put the result into the result queue
        # when finished.
        while not self.task_queue.empty():
            task = self.task_queue.get()
            result = self.worker.run(task)
            self.result_queue.put(result)

        # Once there is no more tasks to draw from the queue, put
        # the worker in the list of idle workers so that the worker
        # manager can assign them to other works.
        self.idle_worker_queue.put(self.worker)

        # Signal to the main process that the current thread is done,
        # and kill this thread.
        self.work_done_event.set()


class WorkProgressTracker(Thread):
    """Monitor the progress of task consumption from the queue and display
    the progress in a progress bar.
    """

    def __init__(self, num_tasks, result_queue, work_done_event, interval=1.0):
        super(WorkProgressTracker, self).__init__()

        self.num_tasks = num_tasks
        self.result_queue = result_queue
        self.work_done_event = work_done_event
        self.interval = interval
        self.pb = ProgressBar(self.num_tasks)

    def run(self):

        while self.result_queue.qsize() < self.num_tasks:

            time.sleep(1.0)
            self.pb.report(self.result_queue.qsize())

        self.work_done_event.set()


class WorkScheduler:
    """Refer to documentation:"""

    def __init__(self, manager, tasks_list):

        # Instantiate a queue of idle workers. Initially, all workers
        # are idle.
        self.idle_worker_queue = Queue()
        for worker in manager.workers:
            self.idle_worker_queue.put(worker)

        # Put the list of list of tasks into a list of queues of tasks.
        # This is because queue are threadsafe, so that multiple threads
        # can grab tasks from the same queue without causing race conditions.
        self.num_tasks = sum([len(tasks) for tasks in tasks_list])
        self.tasks_queues = []
        for tasks in tasks_list:
            task_queue = Queue()
            for task in tasks:
                task_queue.put(task)
            self.tasks_queues.append(task_queue)

        # Start the task queues position at -1. We will increment this
        # by 1 each time we do round robin to select the next batch to
        # assign a worker to.
        self.task_queues_pos = -1

        # All results are aggregated to this result queue. This may cause
        # a bit of inefficiency since several hundred workers are writing
        # to the same queue. We will optimize later if we need to, but refrain
        # from over optimizing from the start.
        self.result_queue = Queue()

        # This is to signal that a thread is done working. This will inform us
        # that all of tasks are consumed from the queues AND that those tasks
        # are completed by the workers.
        self.work_done_events = []

    def next_task_queue(self):
        """We round robin the queues of tasks starting from the
        most expensive one to less expensive ones. The assumption is
        that the self.tasks_queues is a list of tasks_queues ordered
        by the workloads.
        """
        self.task_queues_pos += 1
        self.task_queues_pos %= len(self.tasks_queues)

        # If any queue is empty, skip it when doing round robin.
        while self.tasks_queues[self.task_queues_pos].empty():

            # If all of the queues are consumed, return the end of tasks
            # string to signal that all the works are consumed. But remember,
            # some tasks are probably still being worked on by some workers
            # that took them out of the queue.
            if all([tasks_queue.empty() for tasks_queue in self.tasks_queues]):
                return "ENDOFTASKS"

            self.task_queues_pos += 1
            self.task_queues_pos %= len(self.tasks_queues)

        return self.tasks_queues[self.task_queues_pos]

    def next_idle_worker(self):
        """Get the next idle worker from the idle worker queue. Since
        queues are LIFO, this will grab the LRU worker. (LRU: least
        recently used)
        """
        # This is a blocking call, meaning this will block the code
        # if the idle worker queue is empty. Eventually this has to
        # release because finishing works means there will be idle
        # workers.
        return self.idle_worker_queue.get()

    def run(self):

        work_done_event = Event()
        self.work_done_events.append(work_done_event)
        self.work_progress_tracker = WorkProgressTracker(
            num_tasks=self.num_tasks,
            result_queue=self.result_queue,
            work_done_event=work_done_event,
        )
        self.work_progress_tracker.setDaemon(True)
        self.work_progress_tracker.start()

        while True:

            task_queue = self.next_task_queue()

            # If there are no more tasks in the queues, break.
            if task_queue == "ENDOFTASKS":
                break

            # If there are still works to be done, grab the next
            # idle worker. This will block until there is an idle
            # worker.
            idle_worker = self.next_idle_worker()

            # This is important because once all the queue is consumed
            # and we break out of the infinite loop, we still need
            # a signal that tells us that the last drawn tasks are
            # finished being worked on.
            work_done_event = Event()

            # Update the events to only include those that are still
            # not set, so that we don't have an ever growing list of
            # events from dead threads.
            self.work_done_events = [
                _work_done_event
                for _work_done_event in self.work_done_events
                if not work_done_event.isSet()
            ] + [work_done_event]

            t = SchedulerWorkerThread(
                worker=idle_worker,
                task_queue=task_queue,
                result_queue=self.result_queue,
                idle_worker_queue=self.idle_worker_queue,
                work_done_event=work_done_event,
            )
            # Make it daemon so that it runs in the background.
            t.setDaemon(True)
            t.start()

        # Wait for every event to be set.
        for work_done_event in self.work_done_events:
            work_done_event.wait()

        # Unload all the results from the result queue to a list.
        self.results = []
        while not self.result_queue.empty():
            self.results.append(self.result_queue.get())

            