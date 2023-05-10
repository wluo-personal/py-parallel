import time
from multiprocessing import Manager, Process
import subprocess
import queue
import ctypes
import gc
from py_parallel.status import Status
from py_parallel.lock import CrossProcessLock


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
        self._process_status_ = []
        self._buffer_credit_ = Manager().Value(
            ctypes.c_long, self.result_capacity_)
        for _ in range(self.n_cpus_):
            v = Manager().Value(ctypes.c_char, Status.idle)
            v.set(Status.idle)
            self._process_status_.append(v)

    def _spawn(self):
        # task executor
        self.processes_ = []
        for id_ in range(self.n_cpus_):
            process = Process(
                target=self._worker,
                args=(self._process_status_[id_],
                      self._q_task_buffer_,
                      self._q_result_,
                      self.EXIT))
            self.processes_.append(process)

        # task producer
        self.process_task_producer_ = Process(
            target=self._task_procuder,
            args=[self._buffer_credit_, self.EXIT, self._q_task_buffer_, self._q_task_])

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




    def _worker(
            self,
            status: Manager().Value,
            q_task_buffer: Manager().Queue,
            q_result: Manager().Queue,
            EXIT:Manager().Value):
        try:
            while not EXIT.value:
                status.set(Status.idle)
                try:
                    task = q_task_buffer.get(block=False)
                except queue.Empty:
                    time.sleep(self._sleep_)
                    continue
                func, kwargs = task
                status.set(Status.busy)
                q_result.put(func(**kwargs))
        finally:
            print("worker final")
            status.set(Status.terminated)


    def add_task(self, func, kwargs):
        self._q_task_.put((func, kwargs))

    def run(self):
        # 1. spawn
        self._spawn()

        # 2. start task distributor
        for process in self.processes_:
            process.start()
        self.process_task_producer_.start()


    def get_all_process_status(self):
        status = [each.value for each in self._process_status_]
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



    def _soft_kill_a_process(self, process: Process):
        process.kill()

    def _force_kill_a_process(self, process: Process):
        if process.is_alive():
            pid = process.pid
            if pid is not None:
                subprocess.run(["kill", "-9", f"{pid}"])

    def empty_a_queue(self, q: Manager().Queue):
        while q.qsize() > 0:
            try:
                q.get(block=False)
            except queue.Empty:
                break







