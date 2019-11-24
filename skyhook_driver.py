#This is an example of dask server side skyhook_driver library.
#Copy to /usr/lib/python2.7/ so that it can be imported
from dask.distributed import Client
import dask.delayed
import pyarrow as pa
from StringIO import StringIO
from pyarrow import csv


class Coordinator:
    limit = 0
    ongoing = 0

    def __init__(self, limit=2):
        self.limit = limit
        self.ongoing = 0

    def increment(self, x=1):
        if self.ongoing + x < self.limit:
            self.ongoing += x
            return True
        return False
    
    def decrement(self):
        if self.ongoing > 0 :
            self.ongoing -= 1
            return True
        return False

    def getleft(self):
        return self.limit - self.ongoing

def mergeTables(fu_arr):
    postprocess(fu_arr)


def exeQuery(filename,ops):
    def execute(command):
        import os
        result = os.popen(command).read()
        return result
    

    client = Client('10.10.1.2:8786')
    commands = preprocess(filename,ops)
    futures = []

    for command in commands:
        future = client.submit(execute, command)
        futures.append(future)
        

    resultCom = postprocess(futures)
    re = resultCom.to_pydict()

    return re

def getSchema(filename):
    def execute(command):
        import os
        result = os.popen(command).read()
        return result

    client = Client('10.10.1.2:8786')
    command = '/mnt/sda4/skyhookdm-ceph/build/bin/run-query --pool ' + filename + ' --num-objs 1  --verbose --limit 0'
    future = client.submit(execute, command)
    result_str = future.result()
    lines = result_str.split('\n')
    tablename = ''
    schema_start = False
    fields = []

    field_types = []
    field_types.append(pa.null())
    field_types.append(pa.int8())
    field_types.append(pa.int16())
    field_types.append(pa.int32())
    field_types.append(pa.int64())
    field_types.append(pa.uint8())
    field_types.append(pa.uint16())
    field_types.append(pa.uint32())
    field_types.append(pa.uint64())
    field_types.append(pa.string())
    field_types.append(pa.string())
    field_types.append(pa.bool_())
    field_types.append(pa.float32())
    field_types.append(pa.float64())
    field_types.append(pa.date32())
    field_types.append(pa.string())
    field_types.append(pa.string())

    
    for line in lines:
        if(len(line)==0):
            schema_start = False

        if('table name' in line):
            words = line.split()
            tablename = words[-1]

        if(schema_start):
            words = line.split()
            field = pa.field(words[-1], field_types[int(words[1])])
            fields.append(field)



        if('data_schema:' in line):
            schema_start = True
    


    schema = pa.schema(fields)

    return schema


def preprocess(filename, ops):
    prog = '/mnt/sda4/skyhookdm-ceph/build/bin/run-query '
    sub_tasks = task_partition(filename)
    commands = []

    for task in sub_tasks:
        comand = prog + task + ' --pool ' + filename
        for op in ops:
            comand = comand + ' --' + op
        commands.append(comand)

    return commands

def task_partition(filename):
    sub_tasks = ['--start-obj 0 --num-objs 1 --wthreads 30 --qdepth 30 --use-cls --header', '--start-obj 1 --num-objs 2 --wthreads 30 --qdepth 30 --use-cls']
    return sub_tasks

def postprocess(futures):
    from StringIO import StringIO
    from pyarrow import csv

    results = ''
    for future in futures:
        result = future.result()
        results = results + result

    data = results.replace('|', ',')
    file = StringIO(data)
    table = csv.read_csv(file)
    return table

