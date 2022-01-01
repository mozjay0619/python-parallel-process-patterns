from dask.distributed import Client, LocalCluster
from dask_yarn import YarnCluster
import time

from .utils import get_host_ip_address
from .utils import s3_upload_dir
from .utils import s3_download_dir
from .utils import s3_delete_object
from .utils import s3_object_exists
from .utils import ProgressBar

TIMEOUT = 30

class BaseWorker:
        
    def __set_root_dirpath__(self, root_dirpath):
        self.__root_dirpath__ = root_dirpath
        return True

class DaskWorkerWrapper:
    
    def __init__(self, worker):
        self.worker = worker
        
    def run(self, *args, **kwargs):
        return self.worker.run(*args, **kwargs).result()
    
    def __set_root_dirpath__(self, root_dirpath):
        return self.worker.__set_root_dirpath__(root_dirpath).result()

class WorkerManager:
    
    def __init__(self, dask_resource_config, local_root_dirpath=None):
        
        self.remote_reps = None
        self.remote_shared_root_dir = None
        self.remote_root_dirpath = None
        self.local_root_dirpath = local_root_dirpath
        self.dask_resource_config = dask_resource_config
    
    def start_workers(self, WorkerCls, *args, **kwargs):
        
        run_method = getattr(WorkerCls, "run", None)
        if not callable(run_method):
            raise NotImplementedError("The Worker class must implement the [ run ] method!")
            
        self.local_client, local_worker_infos = self.start_local_client(self.dask_resource_config)
        self.local_workers = self.start_local_workers(local_worker_infos, WorkerCls, *args, **kwargs)
        
        if self.dask_resource_config["use_yarn_cluster"]:
            self.yarn_client, remote_worker_infos = self.start_remote_client(self.dask_resource_config)
            self.remote_workers = self.start_remote_workers(remote_worker_infos, WorkerCls, *args, **kwargs)
        else:
            self.remote_workers = []

        self.workers = self.remote_workers + self.local_workers
        
    def start_remote_client(self, dask_resource_config):
        
        print("\nStarting remote Dask client...")
        
        yarn_cluster = YarnCluster(
            n_workers=dask_resource_config["remote_n_workers"],
            worker_vcores=dask_resource_config["remote_worker_vcores"],
            worker_memory=dask_resource_config["remote_worker_memory"],
            environment="python:///usr/bin/python3",
        )
        
        remote_worker_infos = yarn_cluster.scheduler_info.get("workers", {})
        remote_worker_infos.keys()
        
        pb = ProgressBar(dask_resource_config["remote_n_workers"])
        
        start_time = time.time()
        while len(remote_worker_infos.keys()) < dask_resource_config["remote_n_workers"]:

            time.sleep(0.1)
            pb.report(len(remote_worker_infos.keys()))
            
            if time.time() - start_time > dask_resource_config["remote_n_workers"]*1.1 + TIMEOUT:
                
                print("\nUnable to allocate {} containers".format(
                    dask_resource_config["remote_n_workers"] - len(remote_worker_infos.keys())))
                
                break
        
        yarn_client = Client(yarn_cluster)
        return yarn_client, remote_worker_infos
    
    def start_remote_workers(self, remote_worker_infos, Worker, *args, **kwargs):
        
        local_dirs = []
        remote_reps = {}
        remote_worker_addrs = []
        for worker_addr, worker_info in remote_worker_infos.items():
            remote_reps[':'.join(worker_addr.split(':')[0:2])] = worker_addr
            remote_worker_addrs.append(worker_addr)
            local_dirs.append(worker_info['local_directory'].split('/'))
            
        shared_strs = []
        while any(local_dirs):
            strs = set([local_dir.pop(0) for local_dir in local_dirs])
            if len(strs)==1:
                shared_strs.append(strs.pop())
            else:
                break
        remote_shared_root_dir = '/'.join(shared_strs)
        
        remote_workers = []

        for remote_worker_addr in remote_worker_addrs:
            future = self.yarn_client.submit(Worker, *args, **kwargs, actor=True, workers=remote_worker_addr)  
            remote_workers.append(future.result())
            
        for worker in remote_workers:
            assert worker.__set_root_dirpath__(self.remote_root_dirpath).result() == True
            
        remote_workers = [DaskWorkerWrapper(worker) for worker in remote_workers]
        
        self.remote_reps = remote_reps
        self.remote_shared_root_dir = remote_shared_root_dir

        return remote_workers

    def start_local_client(self, dask_resource_config):
        
        print("Starting local Dask client...")
        
        local_cluster = LocalCluster(
            n_workers=dask_resource_config["local_n_workers"],
            threads_per_worker=dask_resource_config["local_threads_per_worker"],
            processes=True,
            host=get_host_ip_address(),
        )
        
        local_worker_infos = local_cluster.scheduler_info.get("workers", {})
        local_worker_infos.keys()
        
        local_client = Client(address=local_cluster, timeout="2s")
        return local_client, local_worker_infos
        
    def start_local_workers(self, local_worker_infos, Worker, *args, **kwargs):
        
        local_worker_addrs = []
        for worker_addr, worker_info in local_worker_infos.items():
            local_worker_addrs.append(worker_addr)
        
        local_workers = []

        for local_worker_addr in local_worker_addrs:
            future = self.local_client.submit(Worker, *args, **kwargs, actor=True, workers=local_worker_addr)  
            local_workers.append(future.result())
            
        for worker in local_workers:
            assert worker.__set_root_dirpath__(self.local_root_dirpath).result() == True
            
        local_workers = [DaskWorkerWrapper(worker) for worker in local_workers]

        return local_workers
    
    def scatter_data(self, target_dir, s3_url, object_name):
        
        self.local_root_dirpath = target_dir
    
        if s3_object_exists(s3_url, object_name):
            raise FileExistsError("That object_name already exists. Please use a different object_name")

        key = upload_dir(target_dir, s3_url, object_name)

        futures = []
        for ip_addr in self.remote_reps.values():
            future = self.yarn_client.submit(
                download_dir, self.remote_shared_root_dir, s3_url, key, workers=ip_addr, pure=False)
            futures.append(future)

        results = self.yarn_client.gather(futures)
        if not len(set(results)) == 1:
            print('Scattering failed: ')
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