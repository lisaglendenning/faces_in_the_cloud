#
# Usage:
#
# master NWORKERS NCHUNKS START STOP COMMAND ARGS
# worker
#


import scheduler

import simplejson as json

import socket
import subprocess
import sys
import time

#############################################################################
#############################################################################

TAGS = [REGISTER_TAG, TASK_TAG] = ['reg', 'task']
EXE = '/root/build/faces'

def master(argv):
    nworkers = int(argv[0])
    nchunks = int(argv[1])
    partition = (int(argv[2]), int(argv[3]))
    command = argv[4]
    command_args = argv[5:]
    cumulative_result = None
    
    master = scheduler.TaskMaster()
    
    # create tasks
    tasks = [ ]
    step = (partition[1] - partition[0] + 1) / nchunks
    for i in range(nchunks):
        chunk = [partition[0] + i*step]
        chunk.append(partition[1] if i==nchunks-1 else chunk[0]+step-1)
        task = { 'id' : i,
                 'tag' : TASK_TAG,
                'command' : command, 
                'command_args' : command_args,
                'chunk' : chunk }
        print "Task: ", task
        tasks.append(master.new_task(task))
    master.start(tasks)
    
    # wait for workers to register
    workers = set()
    while len(workers) < nworkers:
        msg = master.poll()
        if msg is None:
            time.sleep(0.5)
            continue
        if msg['tag'] == REGISTER_TAG and msg['id'] not in workers:
            workers.add(msg['id'])
    print "Workers: ", workers
    
    # main loop
    start = time.time()
    while not master.done():
        result = master.next()
        if result is None:
            time.sleep(0.001)
            continue
        if result['tag'] == TASK_TAG:
            cumulative_result = execute_result(result, master, cumulative_result)
            print "Result: ", result.get_body()
            print "Cumulative: ", cumulative_result
    master.stop()

    stop = time.time()
    elapsed = stop - start
    print elapsed
    print cumulative_result


def execute_result(result, master, cumulative_result):
    if result['command'] == 'learn':
        return None
    else:
        # query
        result = json.loads(result['output'])
        for queryid in result:
            min = result[queryid][0]
            if cumulative_result is None or min[1] < cumulative_result[1]:
                return min
            return cumulative_result

#############################################################################
#############################################################################

def worker(argv):
    worker = scheduler.TaskWorker()
    worker.start()
    
    # register
    workerid = socket.gethostname()
    msg = worker.new_result({ 'tag' : REGISTER_TAG, 'id' : workerid })
    worker.put_result(msg)
    
    # main polling loop
    while True:
        task = worker.next()
        result = execute_task(task)
        msg = worker.new_result(result)
        worker.complete(task, msg)


def execute_task(task):
    args = [EXE, task['command']] 
    args += task['command_args']
    args += [str(c) for c in task['chunk']]
    print "Executing: ", task.get_body(), " as ", args
    child = subprocess.Popen(args, stdout=subprocess.PIPE)
    output = child.communicate()
    
    result = { }
    result.update(task.get_body())
    result['output'] = output[0][:-1]
    print "Result: ", result
    return result

#############################################################################
#############################################################################

def main(argv):
    if argv[1] == "master":
        master(argv[2:])
    else:
        worker(argv[2:])


if __name__ == "__main__":
    main(sys.argv)

#############################################################################
#############################################################################
