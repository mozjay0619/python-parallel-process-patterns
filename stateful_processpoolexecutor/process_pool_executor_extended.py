from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import _sendback_result, _global_shutdown

# codes from https://github.com/python/cpython/blob/v3.7.13/Lib/concurrent/futures/process.py

# only the lines with "extended" code comment is modified from the original codes.

import atexit
import os
from concurrent.futures import _base
import queue
from queue import Full
import multiprocessing as mp
from multiprocessing.connection import wait
from multiprocessing.queues import Queue
import threading
import weakref
from functools import partial
import itertools
import sys
import traceback


_threads_wakeups = weakref.WeakKeyDictionary()
_global_shutdown = False


class _WorkItem(object):
    def __init__(self, stateful, future, fn, args, kwargs):
        self.stateful = stateful  # extended
        self.future = future
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

class _CallItem(object):
    def __init__(self, stateful, work_id, fn, args, kwargs):
        self.stateful = stateful  # extended
        self.work_id = work_id
        self.fn = fn
        self.args = args
        self.kwargs = kwargs


def _process_worker(self, call_queue, result_queue, initializer, initargs):
    """Evaluates calls from call_queue and places the results in result_queue.
    This worker is run in a separate process.
    Args:
        call_queue: A ctx.Queue of _CallItems that will be read and
            evaluated by the worker.
        result_queue: A ctx.Queue of _ResultItems that will written
            to by the worker.
        initializer: A callable initializer, or None
        initargs: A tuple of args for the initializer
    """
    if initializer is not None:
        try:
            initializer(*initargs)
        except BaseException:
            _base.LOGGER.critical('Exception in initializer:', exc_info=True)
            # The parent will notice that the process stopped and
            # mark the pool broken
            return
    while True:
        call_item = call_queue.get(block=True)
        if call_item is None:
            # Wake up queue management thread
            result_queue.put(os.getpid())
            return
        try:
            if call_item.stateful:  # extended
                r = call_item.fn(self, *call_item.args, **call_item.kwargs)
            else:
                r = call_item.fn(*call_item.args, **call_item.kwargs)
        except BaseException as e:
            exc = _ExceptionWithTraceback(e, e.__traceback__)
            _sendback_result(result_queue, call_item.work_id, exception=exc)
        else:
            _sendback_result(result_queue, call_item.work_id, result=r)

        # Liberate the resource as soon as possible, to avoid holding onto
        # open files or shared memory that is not needed anymore
        del call_item


def _add_call_item_to_queue(pending_work_items,
                            work_ids,
                            call_queue):
    """Fills call_queue with _WorkItems from pending_work_items.
    This function never blocks.
    Args:
        pending_work_items: A dict mapping work ids to _WorkItems e.g.
            {5: <_WorkItem...>, 6: <_WorkItem...>, ...}
        work_ids: A queue.Queue of work ids e.g. Queue([5, 6, ...]). Work ids
            are consumed and the corresponding _WorkItems from
            pending_work_items are transformed into _CallItems and put in
            call_queue.
        call_queue: A multiprocessing.Queue that will be filled with _CallItems
            derived from _WorkItems.
    """
    while True:
        if call_queue.full():
            return
        try:
            work_id = work_ids.get(block=False)
        except queue.Empty:
            return
        else:
            work_item = pending_work_items[work_id]

            if work_item.future.set_running_or_notify_cancel():
                call_queue.put(_CallItem(work_item.stateful,  # extended
                                         work_id,
                                         work_item.fn,
                                         work_item.args,
                                         work_item.kwargs),
                               block=True)
            else:
                del pending_work_items[work_id]
                continue


def _queue_management_worker(executor_reference,
                             processes,
                             pending_work_items,
                             work_ids_queue,
                             call_queue,
                             result_queue,
                             thread_wakeup):
    """Manages the communication between this process and the worker processes.
    This function is run in a local thread.
    Args:
        executor_reference: A weakref.ref to the ProcessPoolExecutor that owns
            this thread. Used to determine if the ProcessPoolExecutor has been
            garbage collected and that this function can exit.
        process: A list of the ctx.Process instances used as
            workers.
        pending_work_items: A dict mapping work ids to _WorkItems e.g.
            {5: <_WorkItem...>, 6: <_WorkItem...>, ...}
        work_ids_queue: A queue.Queue of work ids e.g. Queue([5, 6, ...]).
        call_queue: A ctx.Queue that will be filled with _CallItems
            derived from _WorkItems for processing by the process workers.
        result_queue: A ctx.SimpleQueue of _ResultItems generated by the
            process workers.
        thread_wakeup: A _ThreadWakeup to allow waking up the
            queue_manager_thread from the main Thread and avoid deadlocks
            caused by permanently locked queues.
    """
    executor = None

    def shutting_down():
        return (_global_shutdown or executor is None
                or executor._shutdown_thread)

    def shutdown_worker():
        # This is an upper bound on the number of children alive.
        n_children_alive = sum(p.is_alive() for p in processes.values())
        n_children_to_stop = n_children_alive
        n_sentinels_sent = 0
        # Send the right number of sentinels, to make sure all children are
        # properly terminated.
        while n_sentinels_sent < n_children_to_stop and n_children_alive > 0:
            for i in range(n_children_to_stop - n_sentinels_sent):
                try:
                    call_queue.put_nowait(None)
                    n_sentinels_sent += 1
                except Full:
                    break
            n_children_alive = sum(p.is_alive() for p in processes.values())

        # Release the queue's resources as soon as possible.
        call_queue.close()
        # If .join() is not called on the created processes then
        # some ctx.Queue methods may deadlock on Mac OS X.
        for p in processes.values():
            p.join()

    result_reader = result_queue._reader
    wakeup_reader = thread_wakeup._reader
    readers = [result_reader, wakeup_reader]

    while True:
        _add_call_item_to_queue(pending_work_items,
                                work_ids_queue,
                                call_queue)

        # Wait for a result to be ready in the result_queue while checking
        # that all worker processes are still running, or for a wake up
        # signal send. The wake up signals come either from new tasks being
        # submitted, from the executor being shutdown/gc-ed, or from the
        # shutdown of the python interpreter.
        worker_sentinels = [p.sentinel for p in processes.values()]
        ready = wait(readers + worker_sentinels)

        cause = None
        is_broken = True
        if result_reader in ready:
            try:
                result_item = result_reader.recv()
                is_broken = False
            except BaseException as e:
                cause = traceback.format_exception(type(e), e, e.__traceback__)

        elif wakeup_reader in ready:
            is_broken = False
            result_item = None
        thread_wakeup.clear()
        if is_broken:
            # Mark the process pool broken so that submits fail right now.
            executor = executor_reference()
            if executor is not None:
                executor._broken = ('A child process terminated '
                                    'abruptly, the process pool is not '
                                    'usable anymore')
                executor._shutdown_thread = True
                executor = None
            bpe = BrokenProcessPool("A process in the process pool was "
                                    "terminated abruptly while the future was "
                                    "running or pending.")
            if cause is not None:
                bpe.__cause__ = _RemoteTraceback(
                    f"\n'''\n{''.join(cause)}'''")
            # All futures in flight must be marked failed
            for work_id, work_item in pending_work_items.items():
                work_item.future.set_exception(bpe)
                # Delete references to object. See issue16284
                del work_item
            pending_work_items.clear()
            # Terminate remaining workers forcibly: the queues or their
            # locks may be in a dirty state and block forever.
            for p in processes.values():
                p.terminate()
            shutdown_worker()
            return
        if isinstance(result_item, int):
            # Clean shutdown of a worker using its PID
            # (avoids marking the executor broken)
            assert shutting_down()
            p = processes.pop(result_item)
            p.join()
            if not processes:
                shutdown_worker()
                return
        elif result_item is not None:
            work_item = pending_work_items.pop(result_item.work_id, None)
            # work_item can be None if another process terminated (see above)
            if work_item is not None:
                if result_item.exception:
                    work_item.future.set_exception(result_item.exception)
                else:
                    work_item.future.set_result(result_item.result)
                # Delete references to object. See issue16284
                del work_item
            # Delete reference to result_item
            del result_item

        # Check whether we should start shutting down.
        executor = executor_reference()
        # No more work items can be added if:
        #   - The interpreter is shutting down OR
        #   - The executor that owns this worker has been collected OR
        #   - The executor that owns this worker has been shutdown.
        if shutting_down():
            try:
                # Flag the executor as shutting down as early as possible if it
                # is not gc-ed yet.
                if executor is not None:
                    executor._shutdown_thread = True
                # Since no new work items can be added, it is safe to shutdown
                # this thread if there are no pending work items.
                if not pending_work_items:
                    shutdown_worker()
                    return
            except Full:
                # This is not a problem: we will eventually be woken up (in
                # result_queue.get()) and be able to send a sentinel again.
                pass
        executor = None


class BrokenProcessPool(_base.BrokenExecutor):
    """
    Raised when a process in a ProcessPoolExecutor terminated abruptly
    while a future was in the running state.
    """
    

class ProcessPoolExecutorExtended(ProcessPoolExecutor):

    def _start_queue_management_thread(self):
        if self._queue_management_thread is None:
            # When the executor gets garbarge collected, the weakref callback
            # will wake up the queue management thread so that it can terminate
            # if there is no pending work item.
            def weakref_cb(_,
                           thread_wakeup=self._queue_management_thread_wakeup):
                mp.util.debug('Executor collected: triggering callback for'
                              ' QueueManager wakeup')
                thread_wakeup.wakeup()
            # Start the processes so that their sentinels are known.
            self._adjust_process_count()
            self._queue_management_thread = threading.Thread(
                target=_queue_management_worker,
                args=(weakref.ref(self, weakref_cb),
                      self._processes,
                      self._pending_work_items,
                      self._work_ids,
                      self._call_queue,
                      self._result_queue,
                      self._queue_management_thread_wakeup),
                name="QueueManagerThread")
            self._queue_management_thread.daemon = True
            self._queue_management_thread.start()
            _threads_wakeups[self._queue_management_thread] = \
                self._queue_management_thread_wakeup
    
    def submit(*args, **kwargs):
        if len(args) >= 2:
            self, fn, *args = args
        elif not args:
            raise TypeError("descriptor 'submit' of 'ProcessPoolExecutor' object "
                            "needs an argument")
        elif 'fn' in kwargs:
            fn = kwargs.pop('fn')
            self, *args = args
        else:
            raise TypeError('submit expected at least 1 positional argument, '
                            'got %d' % (len(args)-1))
            
        stateful = kwargs.pop('__stateful__')

        with self._shutdown_lock:
            if self._broken:
                raise BrokenProcessPool(self._broken)
            if self._shutdown_thread:
                raise RuntimeError('cannot schedule new futures after shutdown')
            if _global_shutdown:
                raise RuntimeError('cannot schedule new futures after '
                                   'interpreter shutdown')

            f = _base.Future()
            w = _WorkItem(stateful, f, fn, args, kwargs)

            self._pending_work_items[self._queue_count] = w
            self._work_ids.put(self._queue_count)
            self._queue_count += 1
            # Wake up queue management thread
            self._queue_management_thread_wakeup.wakeup()

            self._start_queue_management_thread()
            return f
    
    def _adjust_process_count(self):
        for _ in range(len(self._processes), self._max_workers):
            p = self._mp_context.Process(
                target=_process_worker,
                args=(self,
                      self._call_queue,
                      self._result_queue,
                      self._initializer,
                      self._initargs))
            p.start()
            self._processes[p.pid] = p
