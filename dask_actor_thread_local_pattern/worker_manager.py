import os
import time
from multiprocessing import set_start_method

from dask.distributed import Client, LocalCluster

from .utils import ProgressBar, get_host_ip_address

TIMEOUT = 12


class BaseWorker:
    def run(self, sleep=0):
        time.sleep(sleep)
        loc = "locally"
        return "Hi! I live {} at {}".format(loc, self.__addr__)

    def __set_root_dirpath__(self, root_dirpath):
        self.__root_dirpath__ = root_dirpath
        return True

    def __get_root_dirpath__(self):
        return self.__root_dirpath__

    def __initialize_worker__(self, addr, root_dirpath):
        self.__addr__ = addr
        self.__root_dirpath__ = root_dirpath
        return True

    def __getaddr__(self):
        return self.__addr__

    def run_method(self, method, *args, **kwargs):
        return method(*args, **kwargs)

    def run_stateful_method(self, method, *args, **kwargs):
        return method(self, *args, **kwargs)


class DaskWorkerWrapper:
    """The purpose of this short wrapper is two folds:
    1. Enforce once more the implementation of "run" method.
    2. Avoid needing to invoke ".result()" on the run method.
    """

    def __init__(self, worker):
        self.worker = worker
        self.__addr__ = worker.__getaddr__().result()
        self.__root_dirpath__ = worker.__get_root_dirpath__().result()

    def __set_root_dirpath__(self, root_dirpath):
        return self.worker.__set_root_dirpath__(root_dirpath).result()

    def run(self, *args, **kwargs):
        return self.worker.run(*args, **kwargs).result()

    def run_method(self, method, *args, **kwargs):
        return self.worker.run_method(method, *args, **kwargs).result()

    def run_stateful_method(self, method, *args, **kwargs):
        return self.worker.run_stateful_method(method, *args, **kwargs).result()

    def run_future(self, *args, **kwargs):
        return self.worker.run(*args, **kwargs)

    def run_method_future(self, method, *args, **kwargs):
        return self.worker.run_method(method, *args, **kwargs)

    def run_stateful_method_future(self, method, *args, **kwargs):
        return self.worker.run_stateful_method(method, *args, **kwargs)

    def __repr__(self):
        
        loc = "Local"

        return "{} Worker at {}".format(loc, self.__addr__)


class WorkerManager:
    """Manages the local and remote (in case of using EMR cluster) Dask Actors (
    http://distributed.dask.org/en/stable/actors.html). In case the EMR cluster
    is used, it defaults to using both the local computer as well as the remote
    computers.
    In cases of large scale computations on remote machines, it is better to
    scatter the dataset and write them on the remote disks ahead of time. This is
    done using "scatter_data" method (Refer to the "scatter_data" method below).
    We do not rely on the Dask's native scatter method because that seems to
    disable the graphical display of the tasks on Dask status page, and so that
    we can have the full control over data scattering protocol. But this requires
    access to S3 buckets.
    But doing so will change the directory where the data can be found. In order
    to normalize the directory, we impose the following contract:
    1. You can only scatter an entire directory instead of individual files. So
    put all the relevant data in a directory before scattering it.
    2. The Worker will receive a special field named "__root_dirpath__" upon
    invoking the scatter_data method. This will point directly to the directory
    that is written on each machine, both local and remote.
    """

    def __init__(self, dask_config, WorkerCls=None, *args, **kwargs):

        self.local_root_dirpath = dask_config.get_config()["root_dirpath"]
        self.dask_config = dask_config.get_config()
        self.local_client = None

        if WorkerCls:
            WorkerCls = WorkerCls
        else:
            WorkerCls = BaseWorker

        self.start_workers(WorkerCls, *args, **kwargs)

    def start_workers(self, WorkerCls, *args, **kwargs):

        set_start_method(self.dask_config["process_start_method"], force=True)

        self.local_client, local_worker_infos = self.start_local_client(
            self.dask_config
        )
        self.local_workers = self.start_local_workers(
            local_worker_infos, WorkerCls, *args, **kwargs
        )

        self.workers = self.local_workers

    def start_local_client(self, dask_config):

        self.close()

        print("Starting local Dask client...\n")

        import dask

        dask.config.set({"multiprocessing.context": "fork"})

        local_cluster = LocalCluster(
            n_workers=dask_config["local_n_workers"],
            threads_per_worker=dask_config["local_threads_per_worker"],
            processes=True,
            host=get_host_ip_address(),
        )

        local_worker_infos = local_cluster.scheduler_info.get("workers", {})
        local_worker_infos.keys()

        local_client = Client(address=local_cluster, timeout="30s")
        return local_client, local_worker_infos

    def start_local_workers(self, local_worker_infos, Worker, *args, **kwargs):

        local_worker_addrs = []
        for worker_addr, worker_info in local_worker_infos.items():
            local_worker_addrs.append(worker_addr)

        local_workers = []

        for local_worker_addr in local_worker_addrs:
            future = self.local_client.submit(
                Worker, *args, **kwargs, actor=True, workers=local_worker_addr
            )
            local_workers.append([future.result(), local_worker_addr])

        for worker, addr in local_workers:
            assert (
                worker.__initialize_worker__(
                    addr, self.local_root_dirpath
                ).result()
                == True
            )

        local_workers = [DaskWorkerWrapper(worker) for worker, addr in local_workers]

        self.local_root_dirpath = os.getcwd()

        return local_workers

    def close(self):

        if self.local_client and not self.local_client.status == "closed":
            print("Closing down previously connected local client and local workers.\n")
            self.local_client.cluster.close()
            self.local_client.close()
