import time
from multiprocessing import Manager, Process
import queue
import ctypes
import gc
from py_parallel.status import Status
from py_parallel.utils import (
    Process as util_process,
    Queue as util_queue
)
from py_parallel.lock import CrossProcessLock
import logging


_ =\
"""
known issue:
Do not use self.X to share value between process. Instead, pass the handle through
args, otherwise, you may have some issue like process 'stopped exitcode=-SIGSEGV'


stopped exitcode=-SIGABRT Fix Solution:
new process to do th spawn. -- use Array to save pid


"""

class DelayedExecutor:
    def __init__(self, n_cpus=1, result_capacity=5000):
        """
        This DelayedExecutor can execute task in new process and the result of
        the task will be saved in a queue. If the size of the queue reaches a
        certain threshold, it stops execute the tasks, until the queue get
        smaller. This is to prevent the memory exhaustion.
        Usage case: CV DNN related realtime image augment + DNN training.

        The result sequence may not be the same as the order you add tasks

        :param n_cpus: number of cpus(hyper-threads)
        :param result_capacity: the threshold of the q

        methods:
         - run
         - add_task
         - get_result
         - get_result_queue_size
         - get_all_process_status
         - close
         - get_all_process_status
         - get_worker_pids

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
        # typecode: https://docs.python.org/3.5/library/array.html
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
            remain = buffer_credit.value
            if remain > 0:
                n_added = 0
                with CrossProcessLock.lock(self._lock):
                    remain = buffer_credit.value
                    # get tasks
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
                    if not util_process.is_process_healthy(pid):
                        logging.error(f"The process index: {process_index}, "
                                      f"pid: {pid} is unhealthy. "
                                      f"Start a new process")
                        try:
                            util_process.kill_process_soft(pid)
                        except Exception:
                            pass
                        util_process.kill_process_force(pid)
                        array_pid[process_index] = 0

        # exit condition
        for process_index in range(self.n_cpus_):
            pid = array_pid[process_index]
            try:
                util_process.kill_process_soft(pid)
            except Exception:
                pass
            util_process.kill_process_force(pid)




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
                try:
                    r = func(**kwargs)
                except Exception as e:
                    self.EXIT.set(True)
                    logging.error("Encounter error in worker. Stopping all workers.")
                    logging.exception(e)
                    raise e
                q_result.put(r)
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
        """
        Get result. It's non-block and the user needs to handle potential exceptions.
        :return:
        """
        res = self._q_result_.get(block=False)
        with CrossProcessLock.lock(self._lock):
            self._buffer_credit_.value += 1
        return res

    def get_result_queue_size(self):
        return self._q_result_.qsize()

    def get_worker_pids(self):
        return list(self._process_ids_[:])



    def close(self):
        """
        release all resources
        :return:
        """
        self.EXIT.set(True)

        # 1. kill workers
        for idx in range(len(self._process_ids_)):
            pid = self._process_ids_[idx]
            try:
                util_process.kill_process_soft(pid)
            except Exception:
                pass
            util_process.kill_process_force(pid)

        # 2. kill worker manager
        util_process.kill_process_soft(self.process_worker_manager_)
        util_process.kill_process_force(self.process_worker_manager_)

        # 3. kill task manager
        util_process.kill_process_soft(self.process_task_producer_)
        util_process.kill_process_force(self.process_task_producer_)

        # 4. destroy queues
        util_queue.empty_a_queue(self._q_task_buffer_)
        util_queue.empty_a_queue(self._q_result_)
        util_queue.empty_a_queue(self._q_task_)
        del self._q_task_buffer_
        del self._q_result_
        del self._q_task_
        del self._lock
        del self._process_ids_
        del self._buffer_credit_
        del self._process_status_

        gc.collect()








