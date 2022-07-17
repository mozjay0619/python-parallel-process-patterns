import json
import os

import psutil


class DaskConfigHelper:
    """Utility class to help configure the Dask resources. Currently
    will just use 1 less than the number of physical cores available.
    """

    def __init__(
        self,
        root_dirpath=None,
        local_n_workers=None,
        local_threads_per_worker=None,
        process_start_method="fork",
        verbose=True,
    ):

        self.process_start_method = process_start_method

        if root_dirpath:
            if not os.path.isabs(root_dirpath):
                self.root_dirpath = os.path.join(os.getcwd(), root_dirpath)
            else:
                self.root_dirpath = root_dirpath
        else:
            self.root_dirpath = os.path.join(os.getcwd(), "DATAIO_TEMP_ROOTDIR")

        config = self.get_config()

        if local_n_workers:
            config["local_n_workers"] = local_n_workers

        if local_threads_per_worker:
            config["local_threads_per_worker"] = local_threads_per_worker

        if verbose:

            print("process_start_method:", config["process_start_method"])
            print("local_n_workers:", config["local_n_workers"])
            print("local_threads_per_worker:", config["local_threads_per_worker"])
            print("root_dirpath:", config["root_dirpath"])

    def get_config(self):

        vcpu = psutil.cpu_count(logical=True)
        pcpu = vcpu // 2

        local_n_workers = pcpu - 1
        local_threads_per_worker = 2

        resource_config = {
            "process_start_method": self.process_start_method,
            "local_n_workers": local_n_workers,
            "local_threads_per_worker": local_threads_per_worker,
            "root_dirpath": self.root_dirpath,
        }

        return resource_config

