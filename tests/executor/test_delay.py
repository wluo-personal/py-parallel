from py_parallel.executor.delay import DelayedExecutor
import time, random

def ff1(x):
    xx = random.randint(1,3)
    time.sleep(xx)
    return xx

def test_DelayedExecutor():
    N_cpu = 5
    exe = DelayedExecutor(n_cpus=N_cpu, result_capacity=40)

    # test 1 - before executing, the pid should be all zeros
    pids = exe.get_worker_pids()
    assert len(pids) == N_cpu
    for each in pids:
        assert each == 0

    exe.run()
    time.sleep(1)

    # test 2 - after running, works can be successfully spawned
    pids = exe.get_worker_pids()
    for each in pids:
        assert each > 0

    # test 3 - add tasks
    for x in range(150):
        exe.add_task(ff1, {"x": x})
    time.sleep(3)
    assert exe.get_result_queue_size() > 0

    # 4. get result
    assert exe.get_result() is not None

