import time
from multiprocessing import Manager, Process
import subprocess
import queue
import ctypes
import gc
import psutil
from py_parallel.status import Status
from py_parallel.lock import CrossProcessLock
import logging


_ =\
"""
known issue:
Do not use self.X to share value between process. Instead, pass the handle through
args, otherwise, you may have some issue like process 'stopped exitcode=-SIGSEGV'


stopped exitcode=-SIGABRT Fix Solution:
new process to do th spawn. -- use Array to save pid

typecode: https://docs.python.org/3.5/library/array.html

"""

class DelayedExecutor:
    def __init__(self, n_cpus=1, result_capacity=5000):
        """
        This DelayedExecutor can execute task in new process and the result of
        the task will be saved in a queue. If the size of the queue reaches a
        certain threshold, it stops execute the tasks, until the queue get
        smaller. This is to protect the memory consumption.
        Usage case: CV DNN related realtime image augment + DNN training.
        :param n_cpus: number of cpus(hyper-threads)
        :param result_capacity: the threshold of the q
        """
        self.n_cpus_ = n_cpus
        self.result_capacity_ = result_capacity
        self.EXIT = Manager().Value(ctypes.c_bool, value=False)
        self._sleep_ = 1e-3
        self._lock = Manager().Value(ctypes.c_bool, False)
        self._init_shared_memory()
         # process not started yet




    def _init_shared_memory(self):
        """
        init
            (1) _q_task_: used to store original tasks
            (2) _q_task_buffer: used to control the pacing
            (3) _q_result_: the result will be saved here.
            (4) _process_status_: save list of status of child process
        :return:
        """
        self._q_task_ = Manager().Queue()
        self._q_task_buffer_ = Manager().Queue()
        self._q_result_ = Manager().Queue()
        self._process_ids_ = Manager().Array(typecode="l", sequence=[0] * self.n_cpus_)
        self._buffer_credit_ = Manager().Value(
            ctypes.c_long, self.result_capacity_)
        self._process_status_ = Manager().Array(
            typecode="u", sequence=["n"] * self.n_cpus_)

    def _spawn_process_manager(self):
        # task producer
        self.process_task_producer_ = Process(
            target=self._task_procuder,
            args=[self._buffer_credit_, self.EXIT, self._q_task_buffer_,
                  self._q_task_])

        # worker producer
        self.process_worker_manager_ = Process(
            target=self._worker_manager,
            args=[self.EXIT, self._process_ids_,
                  self._process_status_, self._q_task_buffer_,
                  self._q_result_])


    def _task_procuder(
            self,
            buffer_credit: Manager().Value,
            EXIT: Manager().Value,
            q_task_buffer: Manager().Queue,
            q_task_:Manager().Queue):
        """
        Control the pacing of the speed
        :param q_task: original task pool
        :param q_task_butter: task to be picked by executors
        :return:
        """
        while not EXIT.value:
            time.sleep(self._sleep_)
            with CrossProcessLock.lock(self._lock):
                remain = buffer_credit.value
                n_added = 0
                for _ in range(remain):
                    try:
                        task = q_task_.get(block=False)
                        q_task_buffer.put(task)
                        n_added += 1
                    except queue.Empty:
                        break
                if n_added > 0:
                    buffer_credit.value -= n_added

    def _worker_manager(
            self,
            EXIT: Manager().Value,
            array_pid: Manager().Array,
            array_status: Manager().Array,
            q_task_buffer: Manager().Queue,
            q_result: Manager().Queue
    ):
        while not EXIT.value:
            time.sleep(self._sleep_)
            for process_index in range(self.n_cpus_):
                pid = array_pid[process_index]
                if pid == 0:
                    process = Process(
                        target=self._worker,
                        args=[process_index, array_status, q_task_buffer,
                              q_result, EXIT])
                    process.start()
                    pid = process.pid
                    array_pid[process_index] = pid
                else:
                    # check process status
                    if not self.is_process_healthy(pid):
                        logging.error(f"The process index: {process_index}, "
                                      f"pid: {pid} is unhealthy. "
                                      f"Start a new process")
                        try:
                            self._soft_kill_a_process(pid)
                        except Exception:
                            pass
                        self._force_kill_a_process(pid)
                        array_pid[process_index] = 0



    def _worker(
            self,
            process_index: int,
            array_status: Manager().Array,
            q_task_buffer: Manager().Queue,
            q_result: Manager().Queue,
            EXIT:Manager().Value):
        try:
            while not EXIT.value:
                array_status[process_index] = Status.idle
                try:
                    task = q_task_buffer.get(block=False)
                except queue.Empty:
                    time.sleep(self._sleep_)
                    continue
                func, kwargs = task
                array_status[process_index] = Status.busy
                q_result.put(func(**kwargs))
        finally:
            logging.debug(f"process index: {process_index} exit.")
            array_status[process_index] = Status.terminated


    def add_task(self, func, kwargs):
        self._q_task_.put((func, kwargs))

    def run(self):
        # spawn
        self._spawn_process_manager()

        # start
        self.process_task_producer_.start()
        self.process_worker_manager_.start()



    def get_all_process_status(self):

        status = self._process_status_[:]
        status = list(map(Status.shortcode_to_longcode, status))
        return status

    def get_result(self):
        res = self._q_result_.get(block=False)
        with CrossProcessLock.lock(self._lock):
            self._buffer_credit_.value += 1
        return res

    def get_result_queue_size(self):
        return self._q_result_.qsize()


    def close(self):
        self.EXIT.set(True)

        # # 1. softkill
        # for process in self.processes_:
        #     self._soft_kill_a_process(process=process)
        # self._soft_kill_a_process(process=self.process_task_producer_)
        #
        # # 2. hardkill
        # for process in self.processes_:
        #     self._force_kill_a_process(process=process)
        # self._force_kill_a_process(process=self.process_task_producer_)
        #
        # # 3. destroy queues
        # self.empty_a_queue(self._q_task_buffer_)
        # self.empty_a_queue(self._q_result_)
        # self.empty_a_queue(self._q_task_)
        # del self._q_task_buffer_
        # del self._q_result_
        # del self._q_task_
        # del self._lock
        #
        # # 4. destroy status
        # # del self._process_status_
        #
        # gc.collect()



    def _soft_kill_a_process(self, process: Process or psutil.Process or int):
        if isinstance(process, Process):
            process.kill()
        else:
            if not isinstance(process, psutil.Process):
                try:
                    process = psutil.Process(process)
                except psutil.NoSuchProcess:
                    return
            process.kill()



    def _force_kill_a_process(self, process: Process or int):
        if isinstance(process, Process):
            if process.is_alive():
                pid = process.pid
                if pid is not None:
                        subprocess.run(["kill", "-9", f"{pid}"])
        else:
            pid = process
            subprocess.run(["kill", "-9", f"{pid}"])

    def empty_a_queue(self, q: Manager().Queue):
        while q.qsize() > 0:
            try:
                q.get(block=False)
            except queue.Empty:
                break

    @staticmethod
    def is_process_healthy(process: int or psutil.Process):
        if isinstance(process, int):
            try:
                process = psutil.Process(process)
            except psutil.NoSuchProcess:
                return False

        if process.status() in Status.unhealthy_status:
            return False
        else:
            return True






