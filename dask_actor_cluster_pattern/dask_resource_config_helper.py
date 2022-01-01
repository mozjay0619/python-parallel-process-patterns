import psutil

INSTANCE_TYPES = {
    "m4.large": {"vCPU": 2, "Mem": 8},
    "m4.xlarge": {"vCPU": 4, "Mem": 16},
    "m4.2xlarge": {"vCPU": 8, "Mem": 32},
    "m4.4xlarge": {"vCPU": 16, "Mem": 64},
    "m4.10xlarge": {"vCPU": 40, "Mem": 160},
    "m4.16xlarge": {"vCPU": 64, "Mem": 256},
    "c4.large": {"vCPU": 2, "Mem": 3.75},
    "c4.xlarge": {"vCPU": 4, "Mem": 7.5},
    "c4.2xlarge": {"vCPU": 8, "Mem": 15},
    "c4.4xlarge": {"vCPU": 16, "Mem": 30},
    "c4.8xlarge": {"vCPU": 36, "Mem": 60},
    "r4.large": {"vCPU": 2, "Mem": 15.25},
    "r4.xlarge": {"vCPU": 4, "Mem": 30.5},
    "r4.2xlarge": {"vCPU": 8, "Mem": 61},
    "r4.4xlarge": {"vCPU": 16, "Mem": 122},
    "r4.8xlarge": {"vCPU": 32, "Mem": 244},
    "r4.16xlarge": {"vCPU": 64, "Mem": 488},
}


class DaskResourceConfigHelper:
    
    def __init__(self, use_aws=False, instance_type=None, use_yarn_cluster=False, node_count=None):
        self.use_aws = use_aws
        self.instance_type = instance_type
        self.use_yarn_cluster = use_yarn_cluster
        self.node_count = node_count
    
    def get_config(self):

        if self.use_aws:
        
            vcpu = INSTANCE_TYPES[self.instance_type]["vCPU"]
            mem = INSTANCE_TYPES[self.instance_type]["Mem"]
            pcpu = vcpu//2 

            local_n_workers = pcpu - 1
            local_threads_per_worker = 2

        else:

            vcpu = psutil.cpu_count(logical=True)
            pcpu = vcpu//2 

            local_n_workers = pcpu - 1
            local_threads_per_worker = 2
        
        if self.use_yarn_cluster:

            remote_n_workers = pcpu * self.node_count 
            remote_worker_vcores = 2
            remote_worker_memory = "{} GiB".format(int(mem/pcpu) - 1)
    
        else:
            
            remote_n_workers = None
            remote_worker_vcores = None
            remote_worker_memory = None
        
        resource_config = {
            "use_aws": self.use_aws,
            "use_yarn_cluster": self.use_yarn_cluster,
            "node_count": self.node_count,
            "local_n_workers": local_n_workers,
            "local_threads_per_worker": local_threads_per_worker,
            "remote_n_workers": remote_n_workers,
            "remote_worker_vcores": remote_worker_vcores,
            "remote_worker_memory": remote_worker_memory
        }
        
        return resource_config
    
