from skyhook_common import *

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