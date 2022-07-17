from ._dask_actor_pool_executor import DaskActorPoolExecutor
from ._python_process_pool_executor import PythonProcessPoolExecutor


def WorkerPool(worker_manager=None, workers=None, n_workers=None):
    """Constructor utility method for the two executors with two different
    backends: Dask and Python multiprocessing.
    If the worker_manager or workers is provided, it will return the executor
    with Dask backend.
    If nothing is given or just the number of workers (n_workers), then the
    Python multiprocessing backend will be used.
    Each have different advantages and disadvantages. Please refer to the
    repsective docstrings for each class.
    """
    if worker_manager or workers:

        return DaskActorPoolExecutor(worker_manager, workers)

    else:

        return PythonProcessPoolExecutor(n_workers)