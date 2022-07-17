import concurrent.futures
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import set_start_method

import numpy as np
import pandas as pd
import psutil

from .utils import ProgressBar


class PythonProcessPoolExecutor:
    """This backend uses Python's native multiprocessing module instead of relying
    on Dask.
    Advantage:
    1. Unix copy-on-write enables zero copy data transfer. Apply methods rely on this.
    2. Fast process initialization.
    Disadvantage:
    1. No stateful processes.
    2. Can't be used with cluster.
    List of available (O) and unavailable (X) methods:
    1. apply_to (O)
    2. apply, apply_keyed (O)
    3. capply, capply_keyed (O)
    4. gapply, gapply_keyed (O)
    5. submit, submit_keyed (O)
    6. submit_foreach (X)
    7. submit_stateful, submit_stateful_keyed (X)
    """

    def __init__(self, n_workers=None):

        if n_workers is None:
            self.n_workers = psutil.cpu_count(logical=False)
        set_start_method("fork", force=True)
        self.executor = ProcessPoolExecutor(max_workers=self.n_workers)

        self.results = dict()
        self.futures = list()
        self.num_tasks = 0
        self.data_io = None
        self.data = None

    def start_executor(self):
        """Start the concurrent futures process pool executor if it is not
        already running.
        """
        if self.executor._processes:
            return

        self.executor = ProcessPoolExecutor(max_workers=self.n_workers)

    @staticmethod
    def _apply_method(key, method_attrs, method, *args, **kwargs):
        """The entry method for the parallelization for apply and capply methods.
        Parameters
        ----------
        key : str
            The key associated with this task.
        method_attrs : list
            The list of attributes associated with the task method.
        method : Python method
            The user method.
        args, kwargs
            The arguments for the user method.
        """
        global __GLOBALDATA__  # Use the global variable data for zero copy. (copy-on-write)
        return key, method_attrs, method(__GLOBALDATA__, *args, **kwargs)

    @staticmethod
    def _gapply_method(key, method_attrs, method, *args, **kwargs):
        """The entry method for the parallelization for gapply methods.
        Parameters
        ----------
        key : str
            The key associated with this task.
        method_attrs : list
            The list of attributes associated with the task method.
        method : Python method
            The user method.
        args, kwargs
            The arguments for the user method.
        """
        global __GLOBALDATA__  # Use the global variable data for zero copy. (copy-on-write)
        groupby = method_attrs[1]
        group_value = method_attrs[2]
        group_data = __GLOBALDATA__[__GLOBALDATA__[groupby] == group_value]
        group_indices = np.asarray(group_data.index)

        return key, method_attrs + [group_indices], method(group_data, *args, **kwargs)

    def apply_to(self, data):
        """Places the data in global variable namespace so that the data is
        accessible by the forked subprocesses. Make sure that the process
        start method is set to "fork".
        Parameters
        ----------
        data : Python object
            The object you wish to process in parallel.
        """
        global __GLOBALDATA__
        __GLOBALDATA__ = data
        self.data = data

    def _apply(self, key, method_attrs, method, *args, **kwargs):
        """Utility method for task submission for all the apply methods (apply,
        capply, gapply). Refer to each of the methods for the descriptions of
        their differences.
        This is where we actually invoke the executor and submit jobs to it.
        """
        self.start_executor()

        if method_attrs[0] != "GAPPLY":  # For apply and capply.

            future = self.executor.submit(
                PythonProcessPoolExecutor._apply_method,
                key,
                method_attrs,
                method,
                *args,
                **kwargs
            )
            self.futures.append(future)

        else:  # For gapply.

            global __GLOBALDATA__
            groupby = method_attrs[1]

            # For gapply, we apply the method to each of the groups.
            for group_value in __GLOBALDATA__[groupby].unique():

                # Append the group value to the original key.
                _key = "___".join([str(key), group_value])

                future = self.executor.submit(
                    PythonProcessPoolExecutor._gapply_method,
                    _key,
                    method_attrs + [group_value],
                    method,
                    *args,
                    **kwargs
                )
                self.futures.append(future)

    def apply(self, method, *args, **kwargs):
        """Apply method takes method whose first parameter is the pandas
        dataframe, which was supplied to the "apply_to" method ahead of time.
        Parameters
        ----------
        method : Python method
            User method. It must have the pandas dataframe as its first parameter.
        args, kwargs
            The arguments to the method except for the pandas dataframe.
        """
        self.num_tasks += 1
        self._apply(self.num_tasks, ["APPLY"], method, *args, **kwargs)

    def apply_keyed(self, key, method, *args, **kwargs):
        """Same as the apply method, but it allows users to specify the custom
        key for this task.
        Parameters
        ----------
        key : Python object
            The user set key associated with this task.
        method : Python method
            User method. It must have the pandas dataframe as its first parameter.
        args, kwargs
            The arguments to the method except for the pandas dataframe.
        """
        self.num_tasks += 1
        self._apply(key, ["APPLY"], method, *args, **kwargs)

    def capply(self, method, *args, **kwargs):
        """Column apply method takes method whose first parameter is the pandas
        dataframe, which was supplied to the "apply_to" method ahead of time.
        The result of this method, if it is either pandas dataframe or pandas
        series, will be automatically appended to the input dataframe.
        Parameters
        ----------
        method : Python method
            User method. It must have the pandas dataframe as its first parameter.
        args, kwargs
            The arguments to the method except for the pandas dataframe.
        """
        self.num_tasks += 1
        self._apply(self.num_tasks, ["CAPPLY"], method, *args, **kwargs)

    def capply_keyed(self, key, method, *args, **kwargs):
        """Same as the capply method, but it allows users to specify the custom
        key for this task.
        Parameters
        ----------
        key : Python object
            The user set key associated with this task.
        method : Python method
            User method. It must have the pandas dataframe as its first parameter.
        args, kwargs
            The arguments to the method except for the pandas dataframe.
        """
        self.num_tasks += 1
        self._apply(key, ["CAPPLY"], method, *args, **kwargs)

    def gapply(self, method, groupby, *args, **kwargs):
        """Group apply method takes not only the method whose first parameter
        is the pandas frame, which was supplied to the "apply_to" method ahead
        of time, but also groupby column name.
        The user method will be applied to each of the groups of the dataframe
        partitions and the results, if they are either pandas dataframes or
        pandas series, will be automatically appended to the input dataframe.
        One restriction is that the method should not change the order of the
        input dataframe or the order of the resulting dataframe or series. This
        is because this method relies on the index of the original dataframe
        to attach the results.
        Parameters
        ----------
        method : Python method
            User method. It must have the pandas dataframe as its first parameter
        groupby : string
            The name of the column that you wish to partition the dataframe by.
        """
        self.num_tasks += 1
        self._apply(self.num_tasks, ["GAPPLY", groupby], method, *args, **kwargs)

    def gapply_keyed(self, key, method, groupby, *args, **kwargs):
        """Same as the gapply method, but it allows users to specify the custom
        key for this task.
        The key will have the group value appended to the results.
        Parameters
        ----------
        key : Python object
            The user set key associated with this task.
        method : Python method
            User method. It must have the pandas dataframe as its first parameter.
        groupby : string
            The name of the column that you wish to partition the dataframe by.
        args, kwargs
            The arguments to the method except for the pandas dataframe.
        """
        self.num_tasks += 1
        self._apply(key, ["GAPPLY", groupby], method, *args, **kwargs)

    @staticmethod
    def _submit_method(key, method_attrs, method, *args, **kwargs):
        """The entry method for the parallelization for submitted methods.
        Parameters
        ----------
        key : str
            The key associated with this task.
        method_attrs : list
            The list of attributes associated with the task method.
        method : Python method
            The user method.
        args, kwargs
            The arguments for the user method.
        """
        return key, method_attrs, method(*args, **kwargs)

    def _submit(self, key, method_attrs, method, *args, **kwargs):
        """Utility method for task submission for all the submitted methods.
        This is where we actually invoke the executor and submit jobs to it.
        """
        self.start_executor()

        future = self.executor.submit(
            PythonProcessPoolExecutor._submit_method,
            key,
            method_attrs,
            method,
            *args,
            **kwargs
        )
        self.futures.append(future)

    def submit(self, method, *args, **kwargs):
        """Invoke this method to submit user methods and arguments.
        Parameters
        ----------
        method : Python method
            The user method.
        args, kwargs
            The arguments for the user method.
        """
        self.num_tasks += 1
        self._submit(self.num_tasks, ["STATELESS"], method, *args, **kwargs)

    def submit_keyed(self, key, method, *args, **kwargs):
        """Same as submit, but it allows users to specify the custom
        key for this task.
        Parameters
        ----------
        key : str
            The key associated with this task.
        method : Python method
            The user method.
        args, kwargs
            The arguments for the user method.
        """
        self.num_tasks += 1
        self._submit(key, ["STATELESS"], method, *args, **kwargs)

    def get(self, key=None, defaults=None, timeout=None):
        """Invoke this method to aggregate the results of the parallel
        computations. If nothing is passed to this method, all of the results
        will be returnd in a list.
        This method will also aggregate the dataframe and series results and
        append to the data if necessary in case capply or gapply methods are invoked.
        Parameters
        ----------
        key : str
            If provided, get will return the result of the task that is associated
            with this key.
        defaults : Python object
            The default return values in case the return value is not available
            from the computations.
        timeout : int
            The number of seconds to wait for the result for a given future.
        """
        pb = ProgressBar(self.num_tasks)

        if len(self.futures) > 0:

            for idx, future in enumerate(
                concurrent.futures.as_completed(self.futures, timeout=timeout)
            ):

                _key, method_attrs, result = future.result()

                if isinstance(result, tuple):
                    result = list(result)

                # If the result is from capply method:
                if method_attrs[0] == "CAPPLY":

                    # If the result is a dataframe or a series, we join
                    # the result to the data.
                    # TODO: do validity checks such as the length of dataframe.
                    if isinstance(result, (pd.DataFrame, pd.Series)):

                        self.data = self.data.join(result)
                        result = None

                    elif isinstance(result, list):

                        for i in range(len(result)):

                            elem = result[i]
                            if isinstance(elem, (pd.DataFrame, pd.Series)):
                                self.data = self.data.join(elem)
                                result[i] = None

                    else:

                        pass

                # If the result is from gapply method:
                elif method_attrs[0] == "GAPPLY":

                    if isinstance(result, pd.Series):

                        colname = result.name
                        if colname not in self.data:
                            self.data[colname] = None

                        # Use the group indices to merge the partial results to the
                        # full dataframe.
                        group_indices = method_attrs[3]
                        self.data.loc[group_indices, colname] = result.values

                    elif isinstance(result, pd.DataFrame):

                        colnames = result.columns
                        for colname in colnames:
                            if colname not in self.data:
                                self.data[colname] = None

                        group_indices = method_attrs[3]
                        self.data.loc[group_indices, colnames] = result.values

                self.results[_key] = result
                pb.report(idx + 1)

            self.futures = []
            self.executor.shutdown(wait=True)

        if key is None:
            if self.data is not None:
                return self.data
            else:
                return list(self.results.values())
        else:
            return self.results.get(key, defaults)

    def clear(self):
        """Clear out the results from past task submissions."""
        self.num_tasks = 0
        self.results = dict()

    def __del__(self):
        """Clean up the global namespace if the apply_to method placed the dataframe
        in the namespace.
        """
        global __GLOBALDATA__
        try:
            del __GLOBALDATA__
        except NameError:
            pass