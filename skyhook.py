from skyhook_common import *
import time

class SkyhookDM:
    def __init__(self):
        self.client = None


    def connect(self, ip):
        addr = ip+':8786'
        client = Client(addr)
        self.client = client

    
    def writeDataset(self, path):
        def runOnDriver(path):
            import skyhook_driver as sd
            res = sd.writeDataset(path)
            return res

        fu = self.client.submit(runOnDriver, path)
        result = fu.result()
        return result

    def getDataset(self, name):
        data = json.loads(open('/users/xweichu/projects/pool/data.json').read())
        files = []
        for item in data['files']:
            file = File(item['name'], item['file_attributes'], item['file_schema'], name)
            files.append(file)

        dataset = Dataset(data['dataset_name'], data['size'], files)
        return dataset
    
    def runQuery(self, obj, querystr):
        if 'File' in str(type(obj)):
            obj_prefix = obj.dataset + '.' + obj.name
            brs = querystr.split(',')
            objnames = []
            for br in brs:
                objnames.append(obj_prefix + '.' + br)

            def runOnDriver(objname):
                time.sleep(3)
                return 1
            
            futures = []

            for objname in objnames:
                futures.append(self.client.submit(runOnDriver,objname))
            
            return self.client.gather(futures)
        return 0
                


