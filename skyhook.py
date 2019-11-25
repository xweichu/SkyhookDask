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
    
    #Events;1.Jet_puId
    #Events;1.SV_x
    #testdata.nano_tree.root.Events;1.Muon_dzErr
    
    def runQuery(self, obj, querystr):
        if 'File' in str(obj):
            obj_prefix = obj.dataset + '.' + 'nano_tree.root'
            brs = querystr.split('project')[1].split()[0].split(',')

            objnames = []
            for br in brs:
                objnames.append(obj_prefix + '.' + br)

            def runOnDriver(objname):
                tmppath = '/users/xweichu/projects/pool/'
                bf = open(tmppath + objname, 'rb')
                reader = pa.ipc.open_stream(bf)
                batches = [b for b in reader]
                table = pa.Table.from_batches(batches)
                return table
            
            futures = []

            for objname in objnames:
                futures.append(self.client.submit(runOnDriver,objname))

            tables = self.client.gather(futures)
            
            return tables
        return 0
                


