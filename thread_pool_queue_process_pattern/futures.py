from multiprocessing import Process, Manager
import multiprocessing
import psutil
import os

from .thread_pool_task_queue import TaskQueueThreadPoolExecutor
from .utils import printProgressBar

class WorkerProcess(Process):
    """Subclasses the Process class. This is to avoid running the parallel
    program using Process Pool. I.e. a new process will be created each time
    and will be killed once complete.

    Parameters
    ----------
    task_key : Python object
            If the key is provided, this key will be used for the DictProxy.

    task_idx : int
            The task index. Used by the progress bar.

    task_results : DictProxy object
            Shared dict proxy object that holds the computation outputs.

    task_errors :  DictProxy object
            Shared dict proxy object that holds the stacktrace of failed tasks.

    task_pids : DictProxy object
            Shared dict that contains the process ids of the subprocesses. Used for
            forced early termination of the job.

    func : Python method
            User method to run in the parallel processes.

    args : Python objects
            The positional arguments for the task method.

    kwargs : Python objects
            The keyword arguments for the task method.
    """

    def __init__(
        self,
        task_key,
        task_idx,
        task_results,
        task_errors,
        task_pids,
        func,
        *args,
        **kwargs,
    ):
        super(WorkerProcess, self).__init__()

        self.task_key = task_key
        self.task_idx = task_idx
        self.task_results = task_results
        self.task_errors = task_errors
        self.task_pids = task_pids

        self.func = func
        self.args = args
        self.kwargs = kwargs

    def run(self):

        key = self.task_key or self.task_idx

        self.task_pids[key] = os.getpid()

        try:

            result = self.func(*self.args, **self.kwargs)

            self.task_results[key] = result
            self.task_errors[key] = None

        except Exception as e:

            self.task_errors[key] = str(e) + " from [ {} ] method".format(
                self.func.__name__
            )
            self.task_results[key] = None


class Futures:
    """The user facing object with the concurrent futures-like API and
    functionality. The user method will run immediately upon submission in
    the background by the daemon processes/threads. It will default to
    fork process start method, since most users will use the Futures with
    the interactively defined methods on Jupyter Notebook. But set it to
    'forkserver' and define the method on a separate file if the method
    contains multithreaded programs, such as XGBoost or LightGBM. Refer to
    set_start_method() method.

    By design, unlike Dask or multiprocessing Pool, it will not rely on
    a standing pool of processes. Instead, it will rely on a standing pool
    of threads, each of which will create/kill processes on demand, which
    is the cleanest way to deal with worker resiliency (resilient by definition
    since it kills/restarts a worker every single time), and memory leakages.
    The inner mechanisms are very simple and are completely exposed to the
    user, which makes it actually debuggable.

    Its failure handling logic is customizable.

    1) If the failure rate is above the failure_rate_thresh and the total
    number of tasks attempted is above total_task_counter_thresh, the
    program will exit early instead of working through all the tasks in
    the queue. (Refer: TaskQueueThreadPoolExecutor, fetch_task_execute())

    2) All of the failed tasks will be tried again for at least the min_retry
    number of times OR if the failure rate is below the retry_failure_rate_thresh.
    If the user knows the code has sporadic failures (e.g. H2O, any IO operations on
    AWS server), set these figures appropriately to retry certain tasks
    for failure resiliency. The number of retries will not exceed the max_retry.

    Therefore, Futures can be used not only as parallelization tool, but
    also as a failure resilient executor of any method that is deemed
    unstable.

    Parameters
    ----------
    n_procs : int
            The number of processes to use. Defaults to the number of physical
            CPU cores available.

    show_progress : boolean
            Set this to True to display the progress bar. Refer to results() method.

    num_task_thresh : int
            The threshold on the total number of tasks completed before the failure
            rate becomes relevant.

    failure_rate_thresh : float
            The threshold on the failure rate to trigger the early exit. If the
            failure rate is above this threshold, the program will exit early.

    retry_failure_rate_thresh : float
            The threshold on the failure rate to trigger the retry. If the failure
            rate is below this threshold, the program will retry the failed tasks.

    min_retry : int
            The minimum number of times to retry the failed tasks regardless of the
            failure rates.

    max_retry : int
            The maximum number of times to retry the failed tasks regardless of the
            failure rates.

    verbose : boolean
    """

    def __init__(
        self,
        n_procs=None,
        show_progress=True,
        num_task_thresh=5,
        failure_rate_thresh=1.0,
        retry_failure_rate_thresh=0.1,
        min_retry=1,
        max_retry=3,
        verbose=False,
    ):

        self.n_procs = n_procs
        self.show_progress = show_progress

        self.set_start_method(
            "fork"
        )  # Defaults to fork start method since many users will
        # use Futures with the methods interactively defined on a Jupyter Notebook.

        self.executor = None

        self.task_results = (
            dict()
        )  # A placeholder dict in case user invokes results() method
        # without having submitted any task. Refer to results() method.

        self.num_task_thresh = num_task_thresh
        self.failure_rate_thresh = failure_rate_thresh
        self.retry_failure_rate_thresh = retry_failure_rate_thresh

        self.min_retry = min_retry
        self.max_retry = max_retry

        self.verbose = verbose

    def initialize_executor(
        self,
        is_retry=False,
        finished_task_counter=0,
        failed_task_counter=0,
        total_task_counter=0,
        failure_rate=0,
    ):
        """Create the executor object. Also (re)initializes all the DictProxy
        objects. Unless the executor is being re-initialized through the
        retry logic (refer: results() method), it will recreate the DictProxy
        and reset the task_idx.

        If the executor is being re-initialized through the retry logic, then
        we want to start with the informed task counters so that we don't throw away
        the finished and failed task counts, which is used for the progress bar.

        Non zero arguments to the various counter parameters are reserved for
        retry logic after the first pass at attempting to complete the given
        tasks. This is so that we don't throw away the counts of success/failure
        during retry.

        Parameters
        ----------
        is_retry : boolean

        finished_task_counter : int
                The number of tasks that were submitted and successfully completed
                by a worker.

        failed_task_counter : int
                The number of tasks that were submitted and were failed due to
                one of three reasons. (Refer to TaskQueueThreadPoolExecutor docstring)

        total_task_counter : int
                Sum of finished_task_counter and failed_task_counter.

        failure_rate : float
                Quotient of failed_task_counter and total_task_counter.
        """
        if self.n_procs is None:

            self.n_procs = psutil.cpu_count(logical=False)

        if not is_retry:

            manager = Manager()

            self.task_results = manager.dict()
            self.task_errors = manager.dict()
            self.task_pids = manager.dict()
            self.task_idx = 0
            self.retry_count = 0

        self.executor = TaskQueueThreadPoolExecutor(
            n_threads=self.n_procs,
            task_pids=self.task_pids,
            task_errors=self.task_errors,
            task_results=self.task_results,
            total_task_counter_thresh=self.num_task_thresh,
            failure_rate_thresh=self.failure_rate_thresh,
            finished_task_counter=finished_task_counter,
            failed_task_counter=failed_task_counter,
            total_task_counter=total_task_counter,
            failure_rate=failure_rate,
            verbose=self.verbose,
        )

    def cleanup_executor(self):
        """Close the executor out and completely release it from memory.

        Returns
        -------
        finished_task_counter : int
                The number of successfully completed tasks.

        total_task_counter : int
                The total number of tasks ran.
        """
        if self.verbose:
            print("calling cleanup")

        self.executor.close()

        finished_task_counter = self.executor.finished_task_counter
        failed_task_counter = self.executor.failed_task_counter
        total_task_counter = self.executor.total_task_counter
        failed_tasks = self.executor.failed_tasks

        if not self.executor.is_thread_alive():
            del self.executor
            self.executor = None
        else:
            raise Exception(
                "Some threads were not successfully terminated."
            )  # This should never happen

        return (
            finished_task_counter,
            failed_task_counter,
            total_task_counter,
            failed_tasks,
        )

    @staticmethod
    def task(
        task_key, task_idx, task_results, task_errors, task_pids, func, *args, **kwargs
    ):
        """The task method that will be passed into the task queue object.
        Starts a daemon process that will work on the user method in a
        separate process.

        Parameters
        ----------
        task_key : Python object
                If the key is provided, this key will be used for the DictProxy.

        task_idx : int
                The task index. Used by the progress bar.

        task_results : DictProxy object
                Shared dict proxy object that holds the computation outputs.

        task_errors :  DictProxy object
                Shared dict proxy object that holds the stacktrace of failed tasks.

        task_pids : DictProxy object
                Shared dict that contains the process ids of the subprocesses. Used for
                forced early termination of the job.

        func : Python method
                User method to run in the parallel processes.

        args : Python objects
                The positional arguments for the task method.

        kwargs : Python objects
                The keyword arguments for the task method.
        """
        subproc = WorkerProcess(
            task_key,
            task_idx,
            task_results,
            task_errors,
            task_pids,
            func,
            *args,
            **kwargs,
        )
        subproc.daemon = True
        subproc.start()
        subproc.join()

    def submit(self, func, *args, **kwargs):
        """Queue the user method and the standing daemon threads will take
        and execute it immediately in the background inside a daemon process.

        Parameters
        ----------
        func : Python method
                User method to run in the parallel processes.

        args : Python objects
                The positional arguments for the task method.

        kwargs : Python objects
                The keyword arguments for the task method.
        """
        if self.executor is None:
            self.initialize_executor()

        # When executor instance is created, the stop event is set() at first so
        # we need to turn this switch on.
        if self.executor.is_stopped():
            self.executor.start_threads()

        # None, self.task_idx are repeated here. It's because the executor needs those
        # information upfront (i.e. key and task_idx) but it's really hard to parse
        # them out of the args without introducing unnecessary complexity.
        self.executor.queue_task(
            None,
            self.task_idx,
            Futures.task,
            None,
            self.task_idx,
            self.task_results,
            self.task_errors,
            self.task_pids,
            func,
            *args,
            **kwargs,
        )
        self.task_idx += 1

    def submit_keyed(self, key, func, *args, **kwargs):
        """Same as the submit method, except that the task key is provided by the
        user. The purpose of this functionality is the easy identification of
        the tasks associated with each result/error. The key can be any Python
        object, including a list of arbitrary objects. E.g. ([region_uuid, datetime])

        Parameters
        ----------
        key: Python object
                This will be used as key for the DictProxy objects.

        func : Python method
                User method to run in the parallel processes.

        args : Python objects
                The positional arguments for the task method.

        kwargs : Python objects
                The keyword arguments for the task method.
        """
        if self.executor is None:
            self.initialize_executor()

        if self.executor.is_stopped():
            self.executor.start_threads()

        self.executor.queue_task(
            key,
            self.task_idx,
            Futures.task,
            key,
            self.task_idx,
            self.task_results,
            self.task_errors,
            self.task_pids,
            func,
            *args,
            **kwargs,
        )
        self.task_idx += 1

    def results(self, incomplete=False, keyed=False):
        """Returns the computation results of the methods that were ran.

        Once called, the program will block until all the remaining tasks
        are completed, unless incomplete is set to True.

        If it is called before all the tasks were cleared from the queue,
        the progress bar will appear.

        If there are failures in the tasks, it will retry those failed
        tasks according to the user specified conditions.

        Parameters
        ----------
        incomplete : boolean
                If set True, it will return the results that are immediately
                available, but not necessarily complete, and will not block.

        keyed : boolean
                If set True, it will return the results of key-result pairs in
                dict.
        """
        if incomplete:
            return self.task_results.values()

        if (
            self.executor is not None
        ):  # Removed "and not self.executor.empty():" if the task finishes too quickly, it will be empty
            # by the time we reach this line and join will not be called, which will lead to hanging.

            if self.show_progress:
                printProgressBar(0, self.task_idx)

            try:

                if self.show_progress:
                    self.executor.set_progress_bar_event()

                self.executor.join()

                (
                    finished_task_counter,
                    failed_task_counter,
                    total_task_counter,
                    failed_tasks,
                ) = self.cleanup_executor()

                # retry logic
                while True:

                    failed_some_tasks = len(failed_tasks) > 0
                    retry_count_below_thresh = self.retry_count < self.min_retry
                    failure_rate_below_thresh = (
                        1 - finished_task_counter / total_task_counter
                    ) <= self.retry_failure_rate_thresh

                    if not (
                        failed_some_tasks
                        and (retry_count_below_thresh or failure_rate_below_thresh)
                    ):

                        break

                    # We want to keep track of how many tasks has been tried/failed until this point so that we
                    # don't throw away the task counts that were successfully completed up to this point. So we
                    # invoke initialize_executor directly instead of relying submit method to do it for us, so
                    # that we can pass in those arguments.
                    self.initialize_executor(
                        is_retry=True,
                        finished_task_counter=finished_task_counter,
                        failed_task_counter=0,  # We are going to retry these so consider them new.
                        total_task_counter=total_task_counter - failed_task_counter,
                    )

                    for key, failed_task in failed_tasks.items():

                        func = failed_task[0]
                        args = failed_task[1]
                        kwargs = failed_task[2]

                        self.submit_keyed(key, func, *args, **kwargs)

                    self.executor.join()

                    (
                        finished_task_counter,
                        failed_task_counter,
                        total_task_counter,
                        failed_tasks,
                    ) = self.cleanup_executor()

                    self.retry_count += 1

                if self.show_progress:
                    printProgressBar(
                        finished_task_counter, total_task_counter, suffix="Completed!"
                    )

            except KeyboardInterrupt:

                self.executor.terminate()

                self.cleanup_executor()

                sys.exit()

            except Exception as e:

                print(type(e), str(e))

            self.failed_tasks = failed_tasks

        if self.task_results and keyed:
            return dict(self.task_results)
        elif self.task_results:
            return self.task_results.values()
        elif keyed:
            warning_msg = "No task was given or executed."
            warnings.warn(warning_msg, NULL_TASK)
            return self.task_results
        else:
            warning_msg = "No task was given or executed."
            warnings.warn(warning_msg, NULL_TASK)
            return self.task_results.values()

    def terminate(self):
        """Call this method if you wish to terminate the daemon processes early,
        but have not yet called the join() on the executor to block the main
        process. Unless join() is called, KeyboardInterrupt will not terminate
        the background daemon threads/processes.
        """
        self.executor.terminate()
        self.cleanup_executor()

    def errors(self):
        """Returns the stacktrace of the tasks."""
        if self.executor is not None and not self.executor.empty():
            self.executor.join()
            self.executor.close()

        return dict(self.task_errors)

    def set_start_method(self, start_method):

        multiprocessing.set_start_method(start_method, force=True)

    def get_start_method(self):

        return multiprocessing.get_start_method()
