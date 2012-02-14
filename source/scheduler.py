#
# The following environment variables must be defined:
# AWS_ACCESS_KEY_ID
# AWS_SECRET_ACCESS_KEY
#

import time

from boto.sqs.connection import SQSConnection
from boto.sqs.jsonmessage import JSONMessage


class TaskSQS:

    TASK_QUEUE = "tasks"
    RESULT_QUEUE = "results" 
    VISIBILITY_TIMEOUT = 120
    
    def __init__(self):
        self.conn = None
        self.taskq = None
        self.resultq = None

    def connect(self):
        # open connection
        self.conn = SQSConnection()
        
        # initialize queues
        self.taskq = self.conn.create_queue(self.TASK_QUEUE, self.VISIBILITY_TIMEOUT)
        self.taskq.set_message_class(JSONMessage)
        self.resultq = self.conn.create_queue(self.RESULT_QUEUE, self.VISIBILITY_TIMEOUT)
        self.resultq.set_message_class(JSONMessage)
    
    def clear(self):
        self.taskq.clear()
        self.resultq.clear()
#        self.conn.delete_queue(self.taskq)
#        self.conn.delete_queue(self.resultq)
        self.taskq = None
        self.resultq = None
    
    def new_task(self, task):
        return JSONMessage(self.taskq, task)
    
    def new_result(self, result):
        return JSONMessage(self.resultq, result)

    def put_task(self, task):
        self.taskq.write(task)
        
    def next_task(self):
        next = None
        while next is None:
            next = self.taskq.read()
        return next
    
    def complete(self, task, result=None):
        self.taskq.delete_message(task)
        if result is not None:
            self.put_result(result)
    
    def put_result(self, result):
        self.resultq.write(result)

    def get_result(self):
        result = self.resultq.read()
        if result is not None:
            self.resultq.delete_message(result)
        return result
    

class TaskMaster:
    
    RESCHEDULE = 300.0 # seconds

    def __init__(self):
        self.sqs = TaskSQS()
        self.tasks = { }
        self.results = { }
        self.incomplete = set()
        self.reschedule_time = None

    def new_task(self, task):
        return self.sqs.new_task(task)

    def start(self, tasks):
        self.sqs.connect()
        for task in tasks:
            self.tasks[task['id']] = task
            self.incomplete.add(task['id'])
            self.results[task['id']] = None
    
    def poll(self):
        return self.sqs.get_result()

    def next(self):
        result = self.sqs.get_result()
        if result is not None and result['tag'] == 'task':
            if result['id'] in self.incomplete:
                self.results[result['id']] = result
                self.incomplete.remove(result['id'])
            else:
                result = None
        
        if self.reschedule_time is None or time.time() >= self.reschedule_time:
            self.schedule()
        
        return result
    
    def done(self):
        return len(self.incomplete) == 0
    
    def schedule(self):
        for taskid in self.incomplete:
            self.sqs.put_task(self.tasks[taskid])
        self.reschedule_time = time.time() + self.RESCHEDULE
    
    def stop(self):
        self.sqs.clear()


class TaskWorker:

    def __init__(self):
        self.sqs = TaskSQS()

    def new_result(self, result):
        return self.sqs.new_result(result)
    
    def start(self):
        self.sqs.connect()
    
    def next(self):
        return self.sqs.next_task()
    
    def complete(self, task, result=None):
        self.sqs.complete(task, result)

    def put_result(self, result):
        self.sqs.put_result(result)
        
