import time

from dask.distributed import Client, LocalCluster
from dask_yarn import YarnCluster

from .utils import (ProgressBar, get_host_ip_address, s3_delete_object,
                    s3_download_dir, s3_object_exists, s3_upload_dir)

TIMEOUT = 10


class BaseWorker:
    def __set_root_dirpath__(self, root_dirpath):
        self.__root_dirpath__ = root_dirpath
        return True
    
    def __initialize_worker__(self, addr, root_dirpath):
        self.__addr__ = addr
        self.__root_dirpath__ = root_dirpath
        return True
    
    def __getaddr__(self):
        return self.__addr__


class DaskWorkerWrapper:
    """The purpose of this short wrapper is two folds:
    1. Enforce once more the implementation of "run" method.
    2. Avoid needing to invoke ".result()" on the run method.
    """

    def __init__(self, worker):
        self.worker = worker
        self.addr = worker.__getaddr__().result()

    def run(self, *args, **kwargs):
        return self.worker.run(*args, **kwargs).result()

    def __set_root_dirpath__(self, root_dirpath):
        return self.worker.__set_root_dirpath__(root_dirpath).result()


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

    def __init__(self, dask_resource_config, local_root_dirpath=None):

        self.remote_reps = None
        self.remote_shared_root_dir = None
        self.remote_root_dirpath = None
        self.local_root_dirpath = local_root_dirpath
        self.dask_resource_config = dask_resource_config
        self.local_client = None
        self.remote_client = None

    def start_workers(self, WorkerCls, *args, **kwargs):

        run_method = getattr(WorkerCls, "run", None)
        if not callable(run_method):
            raise NotImplementedError(
                "The Worker class must implement the [ run ] method!"
            )

        self.local_client, local_worker_infos = self.start_local_client(
            self.dask_resource_config
        )
        self.local_workers = self.start_local_workers(
            local_worker_infos, WorkerCls, *args, **kwargs
        )

        if self.dask_resource_config["use_yarn_cluster"]:
            self.remote_client, remote_worker_infos = self.start_remote_client(
                self.dask_resource_config
            )
            self.remote_workers = self.start_remote_workers(
                remote_worker_infos, WorkerCls, *args, **kwargs
            )
        else:
            self.remote_workers = []

        self.workers = self.remote_workers + self.local_workers

    def start_remote_client(self, dask_resource_config):
        
        self.close_remote_client()

        print("Starting remote Dask client...")

        yarn_cluster = YarnCluster(
            n_workers=dask_resource_config["remote_n_workers"],
            worker_vcores=dask_resource_config["remote_worker_vcores"],
            worker_memory=dask_resource_config["remote_worker_memory"],
            environment="python:///usr/bin/python3",
        )

        remote_worker_infos = yarn_cluster.scheduler_info.get("workers", {})
        remote_worker_infos.keys()

        pb = ProgressBar(dask_resource_config["remote_n_workers"])

        last_time_worker_was_added = time.time()
        prev_worker_cnt = 0
        while (
            len(remote_worker_infos.keys()) < dask_resource_config["remote_n_workers"]
        ):

            time.sleep(0.1)
            pb.report(len(remote_worker_infos.keys()))
            
            if prev_worker_cnt != len(remote_worker_infos.keys()):
                last_time_worker_was_added = time.time()
                prev_worker_cnt = len(remote_worker_infos.keys())

            if (time.time() - last_time_worker_was_added > TIMEOUT):

                print(
                    "\n(Unable to allocate {} containers)".format(
                        dask_resource_config["remote_n_workers"]
                        - len(remote_worker_infos.keys())
                    )
                )

                break

        remote_client = Client(yarn_cluster)
        return remote_client, remote_worker_infos

    def start_remote_workers(self, remote_worker_infos, Worker, *args, **kwargs):

        local_dirs = []
        remote_reps = {}
        remote_worker_addrs = []
        for worker_addr, worker_info in remote_worker_infos.items():
            remote_reps[":".join(worker_addr.split(":")[0:2])] = worker_addr
            # This remote address is ip address and port number of the worker.
            remote_worker_addrs.append(worker_addr)
            # The path of the local directory of each worker split into list by "/".
            # Used below to get the longest common substring.
            local_dirs.append(worker_info["local_directory"].split("/"))

        # Get the longest common substring, where this represents the root directory
        # that is shared by all worker process in a machine.
        shared_strs = []
        while any(local_dirs):
            strs = set([local_dir.pop(0) for local_dir in local_dirs])
            if len(strs) == 1:
                shared_strs.append(strs.pop())
            else:
                break
        remote_shared_root_dir = "/".join(shared_strs)

        remote_workers = []

        for remote_worker_addr in remote_worker_addrs:
            future = self.remote_client.submit(
                Worker, *args, **kwargs, actor=True, workers=remote_worker_addr
            )
            remote_workers.append([future.result(), remote_worker_addr])

        for worker, addr in remote_workers:
            assert (
                worker.__initialize_worker__(addr, self.remote_root_dirpath).result() == True
            )

        remote_workers = [DaskWorkerWrapper(worker) for worker, addr in remote_workers]

        self.remote_reps = remote_reps
        self.remote_shared_root_dir = remote_shared_root_dir

        return remote_workers

    def start_local_client(self, dask_resource_config):
        
        self.close_local_client()

        print("Starting local Dask client...\n")

        local_cluster = LocalCluster(
            n_workers=dask_resource_config["local_n_workers"],
            threads_per_worker=dask_resource_config["local_threads_per_worker"],
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
            assert worker.__initialize_worker__(addr, self.local_root_dirpath).result() == True

        local_workers = [DaskWorkerWrapper(worker) for worker, addr in local_workers]

        return local_workers

    def scatter_data(self, target_dir, s3_url, object_name):
        """Sends the directory, whose path is "target_dir" argument, is written to the
        disk that is local to each remote machine. Since there may be more than one
        worker process per machine, we write the data to a directory that is common to
        all processes on that machine. Therefore, we only need to ask one process per
        machine to carry out this task, and we can have them all do it in parallel on
        their respective machines.

        The process is as follows:
        1. Zip the local directory.
        2. Upload the zipped file to S3 bucket.
        3. From each remote machine, download the zipped file and unzip it. (done by
        one worker process per machine)
        4. Erase the S3 bucket file.
        5. Update the "__root_dirpath__" field of each workers. (done for all worker
        process in each machine, local as well as remote)
        """
        self.local_root_dirpath = target_dir

        if s3_object_exists(s3_url, object_name):
            raise FileExistsError(
                "That object_name already exists. Please use a different object_name"
            )

        key = s3_upload_dir(target_dir, s3_url, object_name)

        futures = []
        for ip_addr in self.remote_reps.values():
            future = self.remote_client.submit(
                s3_download_dir,
                self.remote_shared_root_dir,
                s3_url,
                key,
                workers=ip_addr,
                pure=False,
            )
            futures.append(future)

        results = self.remote_client.gather(futures)
        if not len(set(results)) == 1:
            print("Scattering failed: ")
            print(results)

        self.remote_root_dirpath = results[0]
        _remote_shared_root_dir, _dirname = os.path.split(self.remote_root_dirpath)
        assert _remote_shared_root_dir == self.remote_shared_root_dir

        assert s3_object_exists(s3_url, object_name)
        s3_delete_object(s3_url, object_name)

        # Assign root_dirpath fields.
        for worker in self.local_workers:
            worker.__set_root_dirpath__(self.local_root_dirpath)

        for worker in self.remote_workers:
            worker.__set_root_dirpath__(self.remote_root_dirpath)

        return self.remote_root_dirpath
    
    def close_local_client(self):
        
        if self.local_client and not self.local_client.status=='closed':
            print("Closing down previously connected local client and local workers.\n")
            self.local_client.cluster.close()
            self.local_client.close()

    def close_remote_client(self):
        
        if self.remote_client and not self.remote_client.status=='closed':
            print("Closing down previously connected remote client and remote workers.\n")
            self.remote_client.cluster.close()
            self.remote_client.close()
    
    def close(self):
        
        self.close_local_client()
        self.close_remote_client()
        