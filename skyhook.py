#This is an example of client side skyhook library.
#Copy to /usr/lib/python2.7/ so that it can be imported

from dask.distributed import Client
import dask.delayed

class Skyhook:
    def __init__(self,ip):
        self.client = self.connect(ip)

    def connect(self, ip):
        addr = ip+':8786'
        client = Client(addr)
        return client

    def query(self, filename, ops):
        def runQuery(filename, ops):
            import skyhook_driver as sd
            re = sd.exeQuery(filename,ops)
            return re

        fu = self.client.submit(runQuery, filename, ops)
        result = fu.result()
        return result