import errno
import os
import pickle
import shutil
import socket
import time
import zipfile
from urllib.parse import urlparse

def get_host_ip_address():
    """Get the host ip address of the machine where the executor is running.
    Returns
    -------
    host_ip : String
    """
    try:
        host_name = socket.gethostname()
        host_ip = socket.gethostbyname(host_name)
        return host_ip
    except:
        host_ip = "127.0.0.1"  # playing with fire...
        return host_ip

def create_dir(dirpath, remove=False, verbose=True):
    """
    Parameters
    ----------
    dirpath : str
        The path or name of the directory to be created
    remove : bool
        If true, will remove directory with the same name
        and create an empty directory with that name.
    Returns
    -------
    dirpath
    """
    if not os.path.isabs(dirpath):
        dirpath = os.path.join(os.getcwd(), dirpath)

    if os.path.exists(dirpath):
        if remove:
            if dirpath == os.getcwd():
                print("If you are going to remove the cwd, please do it yourself.")
                return False
            shutil.rmtree(dirpath)
            if verbose:
                print("Deleting directory: {}\n".format(dirpath))

        else:
            if dirpath == os.getcwd():
                print("Using the current working directory as the root.")
                return False
            if verbose:
                print("Reusing directory: {}\n".format(dirpath))
            return False

    os.makedirs(dirpath)
    if verbose:
        print("Creating directory: {}\n".format(dirpath))
    return True


def save_object(obj, dirpath, filename):
    """Saves arbitrary Python objects using pickle module.
    Parameters
    ----------
    obj : Python object
        The object you wish to save on disk.
    dirpath : str
        The path of the directory to save the object in.
    filename : str
        The name of the object file.
    """
    filepath = os.path.join(dirpath, filename)

    with open(filepath + ".pkl", "wb") as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)


def load_object(dirpath, filename):
    """Retrieve the Python object written as binary.
    Parameters
    ----------
    dirpath : str
        The path of the directory the object is written in.
    filename : str
        The name of the object file.
    """
    filepath = os.path.join(dirpath, filename)

    with open(filepath + ".pkl", "rb") as f:
        return pickle.load(f)


class ProgressBar:
    def __init__(self, total):

        self.start_time = time.time()
        self.total = total

    def report(self, iteration):

        if iteration < self.total:
            self.print_progress_bar(iteration, self.total)
        else:
            self.print_progress_bar(self.total, self.total, suffix="Completed!")

    def print_progress_bar(self, iteration, total, suffix="", length=50, fill="█"):

        dur = time.time() - self.start_time
        time_str = time.strftime("%H:%M:%S", time.gmtime(dur))

        percent = ("{0:.1f}").format(100 * (iteration / float(total)))
        filledLength = int(length * iteration // total)
        bar = fill * filledLength + "-" * (length - filledLength)
        print(f"\rProgress: |{bar}| {percent}% | {time_str}  {suffix}", end="\r")
        if iteration == total and suffix == "Completed!":
            print(f"\r", end="\n")