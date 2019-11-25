#This is an example of client side skyhook library.
#Copy to /usr/lib/python2.7/ so that it can be imported
from dask.distributed import Client
import dask.delayed
from pyarrow import csv
import pyarrow

class SkyhookDM:
    def __init__(self):
        self.client = None

    def connect(self, ip):
        addr = ip+':8786'
        client = Client(addr)
        self.client = client

    def query(self, filename, ops):
        def runQuery(filename, ops):
            import skyhook_driver as sd
            re = sd.exeQuery(filename,ops)
            return re

        fu = self.client.submit(runQuery, filename, ops)
        result = fu.result()
        tb = pyarrow.Table.from_pydict(result)
        return tb
    
    # def getSchema(self, filename):
    #     def runQuery(filename):
    #         import skyhook_driver as sd
    #         re = sd.getSchema(filename)
    #         return re

    #     fu = self.client.submit(runQuery, filename)
    #     result = fu.result()
    #     return result