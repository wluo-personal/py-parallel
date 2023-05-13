import psutil
from py_parallel.status import Status
import multiprocessing as mp
import subprocess
import queue
class Process:
    @staticmethod
    def is_process_healthy(process: int or psutil.Process):
        """
        Check whether a process is in healthy condition.
        :param process:
        :return:
        """
        if isinstance(process, int):
            try:
                process = psutil.Process(process)
            except psutil.NoSuchProcess:
                return False

        if process.status() in Status.unhealthy_status:
            return False
        else:
            return True

    @staticmethod
    def kill_process_soft(process: mp.Process or psutil.Process or int):
        """
        Kill a process by using the process instance native methods
        :param process:
            can be mp.Process, psutil.Process or int
        :return:
        """
        if isinstance(process, mp.Process):
            process.kill()
        else:
            if not isinstance(process, psutil.Process):
                try:
                    process = psutil.Process(process)
                except psutil.NoSuchProcess:
                    return
            process.kill()

    @staticmethod
    def kill_process_force(process: mp.Process or psutil.Process or int):
        """
        Kill a process by using it's pid. This is supported on *nix OS.
        :param process:
        :return:
        """
        if isinstance(process, mp.Process):
            if process.is_alive():
                pid = process.pid
                if pid is not None:
                    subprocess.run(["kill", "-9", f"{pid}"])
        elif isinstance(process, psutil.Process):
            pid = process.pid
            subprocess.run(["kill", "-9", f"{pid}"])

        else:
            pid = process
            subprocess.run(["kill", "-9", f"{pid}"])

class Queue:
    @staticmethod
    def empty_a_queue(q: queue.Queue or mp.Manager().Queue):
        """
        clear the content in a queue
        :param q:
        :return:
        """
        while q.qsize() > 0:
            try:
                q.get(block=False)
            except queue.Empty:
                break