from queue import Queue
import warnings
import time
import os
import signal
import warnings
from threading import Event, Lock
import threading

from .utils import printProgressBar

def format_Warning(message, category, filename, lineno, line=""):
    return (
        str(filename)
        + ":"
        + str(lineno)
        + ": "
        + category.__name__
        + ": "
        + str(message)
        + "\n"
    )


class TASK_FAILED(UserWarning):
    pass


class NULL_TASK(UserWarning):
    pass


class TaskQueueThreadPoolExecutor(Queue):
    """The user tasks are queued into this object, which starts daemon
    threads that fetches those tasks, executes them, and blocks until
    the current task is completed. Once the current task is completed,
    the threads will fetch new tasks until the queue is empty. It will
    block until more tasks are submitted by the user.

    The tasks are executed immediately upon queueing in the background
    by the daemon processes, which blocks the current background daemon
    thread that it is running in, and therefore behaves like the concurrent
    futures.

    It distinguishes three types of errors and raises appropriate
    warnings for each type of errors:

    1) the multiprocessing start method misconfiguration
    2) the premature death of the subprocess
    3) the method exception

    Its failure handling logic is customizable. If the failure rate is
    above failure_rate_thresh and the total number of tasks attempted is
    above total_task_counter_thresh, the program will exit early instead
    of working through all the tasks still in the queue.

    Refer to fetch_task_execute method.

    Parameters
    ----------
    n_threads : int
            The number of deamon threads listening. Set to the number of processes.

    task_pids : DictProxy object
            The dict that contains the process ids of the subprocesses. Used for
            forced early termination of the job.

    task_errors : DictProxy object
            The dict that will record the stacktrace raised by the method exception.
            Default value is None.

    task_results : DictProxy object
            The dict that will record the computation results of the method.

    total_task_counter_thresh : int
            The threshold on the total number of tasks completed before the failure
            rate becomes relevant.

    failure_rate_thresh : float
            The threshold on the failure rate to trigger the early exit.

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

    def __init__(
        self,
        n_threads,
        task_pids,
        task_errors,
        task_results,
        total_task_counter_thresh,
        failure_rate_thresh,
        finished_task_counter=0,
        failed_task_counter=0,
        total_task_counter=0,
        failure_rate=0,
        verbose=False,
    ):
        super(TaskQueueThreadPoolExecutor, self).__init__()

        self.n_threads = n_threads
        self.threads = list()

        self.stop_event = Event()
        self.stop_event.set()
        self.early_stop_event = Event()
        self.sync_lock = Lock()
        self.progress_bar_event = Event()

        self.finished_task_counter = finished_task_counter
        self.failed_task_counter = failed_task_counter
        self.total_task_counter = total_task_counter
        self.failure_rate = failure_rate

        self.total_task_counter_thresh = total_task_counter_thresh
        self.failure_rate_thresh = failure_rate_thresh

        self.task_pids = task_pids
        self.task_errors = task_errors
        self.task_results = task_results

        self.failed_tasks = dict()

        self.verbose = verbose

    def start_threads(self):
        """Start the n_threads deamon threads that fetches, executes, and
        waits for the user queued tasks to complete. These threads will
        continue running until close() method is called.
        """
        if self.is_thread_alive():
            raise Exception("Some threads were not successfully terminated.")

        self.stop_event.clear()

        for _ in range(self.n_threads):

            t = threading.Thread(target=self.fetch_task_execute)
            t.setDaemon(True)
            t.start()
            self.threads.append(t)

        # Reset the warnings module filter at each thread start instance so that
        # these warnings are not just produced once and not again.
        warnings.simplefilter("default")

        # IPython has a bug that triggers DeprecationWarning. We suppress
        # this warning for that reason. Consider removing it in future iteration.
        warnings.simplefilter("ignore", DeprecationWarning)

    def queue_task(self, task_key, task_idx, task, *args, **kwargs):
        """Queue new user tasks into the queue. The daemon threads will
        grab these tasks one at a time when any of them becomes free.

        Parameters
        ----------
        task_key : Python object
                If the key is provided, this key will be used for the DictProxy.

        task_idx : int
                The task index. Used by the progress bar.

        task : Python method
                The user method to be executed by the daemon threads.

        args : Python objects
                The positional arguments for the task method.

        kwargs : Python objects
                The keyword arguments for the task method.
        """
        self.task_key = task_key
        self.task_idx = task_idx
        self.put((task, args, kwargs))

    def fetch_task_execute(self):
        """While the threads are not signalled to stop by the stop_event,
        this method will be continuously ran by the daemon threads.
        It blocks until the user task is finished running.

        It distinguishes three types of errors and raises appropriate
        warnings for each type of errors:

        1) the multiprocessing start method misconfiguration
        2) the premature death of process
        3) the method exception

        Its failure handling logic is customizable. If the failure rate is
        above failure_rate_thresh and the total number of tasks attempted is
        above total_task_counter_thresh, the program will exit early instead
        of working through all the tasks in the queue.
        """
        while not self.stop_event.isSet():

            # self.task_idx >=0 is meant to ignore the case of dummy_method
            # argument by the close method, where the task_idx is -1.
            if (
                self.total_task_counter > self.total_task_counter_thresh
                and self.failure_rate == 1.0
                and self.task_idx >= 0
            ):

                self.stop_event.set()
                self.early_stop_event.set()

            if self.progress_bar_event.isSet() and self.task_idx >= 0:

                printProgressBar(self.finished_task_counter, self.task_idx + 1)

            task, args, kwargs = self.get(block=True)

            task(*args, **kwargs)

            if self.task_idx >= 0:

                key = args[0] or args[1]

                # This means the DictProxy is not functioning at all. This can occur only with the
                # multiprocessing start method misconfiguration.
                if key not in self.task_pids:

                    with self.sync_lock:

                        warning_msg = 'Please set multiprocessing start method to "fork" or move the function definition to a separate .py file and import it.'

                        warnings.warn(warning_msg, TASK_FAILED)

                    self.failed_task_counter += 1

                    self.failed_tasks[key] = (args[5], args[6:], kwargs)

                # If the key is found in task_pids DictProxy, but not the task_errors DictProxy,
                # that means the process started but never reached the end of that code block.
                elif key not in self.task_errors:

                    with self.sync_lock:

                        warning_msg = (
                            "task id {} failed due to death of the process".format(key)
                        )

                        warnings.warn(warning_msg, TASK_FAILED)

                    self.failed_task_counter += 1

                    self.failed_tasks[key] = (args[5], args[6:], kwargs)

                # If the key is found in the task_errors DictProxy, but the
                # value in the corresponding key in the task_errors DictProxy is not None,
                # that means that there was a traceback due to exception.
                elif self.task_errors[key] is not None:

                    with self.sync_lock:

                        warning_msg = "task id {} failed due to: {}".format(
                            key, self.task_errors[key]
                        )

                        warnings.warn(warning_msg, TASK_FAILED)

                    self.failed_task_counter += 1

                    self.failed_tasks[key] = (args[5], args[6:], kwargs)

                # The value in the task_errors DictProxy is found and it is None. The
                # task was successfully completed.
                else:

                    self.finished_task_counter += 1

            self.total_task_counter = (
                self.finished_task_counter + self.failed_task_counter
            )

            if self.total_task_counter > 0:
                self.failure_rate = self.failed_task_counter / self.total_task_counter
            else:
                self.failure_rate = 0

            self.task_done()

        # In case of forced early termination of the program, we want to
        # flush all existing items from the queue.
        if self.early_stop_event.isSet():

            while not self.empty():

                # Use synchronization lock to avoid racing condition where
                # multiple threads invoke get() from an already emptied queue, which
                # will result in hanging.
                with self.sync_lock:

                    self.get()
                    self.task_done()

    def is_stopped(self):
        """Checks if the stop event has been triggered."""
        return self.stop_event.isSet()

    def is_thread_alive(self):
        """Checks if there are any threads that are still running."""
        for thread in self.threads:

            if thread.is_alive():
                return True

        return False

    def n_threads_alive(self):
        """Returns the number of threads that are still running."""
        return len([thread for thread in self.threads if thread.is_alive()])

    def set_progress_bar_event(self):
        """Trigger the progress bar event."""
        self.progress_bar_event.set()

    def clear_progress_bar_event(self):
        """Clear the progress bar event."""
        self.progress_bar_event.clear()

    def close(self):
        """The stop event is triggered to break the infinite while loop in
        fetch_task_execute method. It passes in n_threads number of dummy
        tasks into the queue to unblock the get() method so that the program can
        reach the while loop condition check (where the stop event will break the loop).

        Also, it will flush any remaining tasks in the queue in case the user
        prematurely terminated the program using KeyboardInterrupt for example.
        """
        if self.verbose:
            print("calling close")

        self.stop_event.set()

        def dummy_method():
            return

        for _ in range(self.n_threads_alive()):
            self.queue_task(None, -1, dummy_method)

        # Wait for the dummy methods to kill the threads.
        while self.is_thread_alive():
            time.sleep(0.1)

        # Flush the queue. This shouldn't be necessary under normal conditions.
        while not self.empty():
            self.get()

    def terminate(self):
        """This is for KeyboardInterrupt event triggered by the user or early
        forced termination by user who invoked terminate() method. It will
        kill all the running processes, and then invoke the close method for
        thread closure.
        """
        if self.verbose:
            print("calling terminate")

        self.stop_event.set()

        for pid in self.task_pids.values():

            # We will try to indiscriminately kill all the processes that ever
            # lived. This is crude (what if it killed processes that it should
            # not kill?) Need improvement. Also, SIGTERM may not be the best
            # way to kill processes.
            try:
                os.kill(pid, signal.SIGTERM)

            except ProcessLookupError:
                pass

            except Exception as e:
                print(type(e), str(e))

        self.close()
