#This is an example of dask server side skyhook_driver library.
#Copy to /usr/lib/python2.7/ so that it can be imported
from dask.distributed import Client
import dask.delayed

def exeQuery(filename,ops):
    def execute(command):
        import os
        result = os.popen(command).read()
        return result

    client = Client('10.10.1.2:8786')

    cmda = '/mnt/sda4/skyhookdm-ceph/build/bin/run-query --start-obj 0 --num-objs 1 --pool ' + filename + ' ' + ops[0] + ' ' + ops[1] + ' --wthreads 30 --qdepth 30 --use-cls --limit 10'
    cmdb = '/mnt/sda4/skyhookdm-ceph/build/bin/run-query --start-obj 1 --num-objs 2 --pool ' + filename + ' ' + ops[0] + ' ' + ops[1] + ' --wthreads 30 --qdepth 30 --use-cls --limit 10'
    future1 = client.submit(execute, cmda)
    future2 = client.submit(execute, cmdb)

    result1 = future1.result()
    result2 = future2.result()

    resultCom = result1 + result2
    return resultCom