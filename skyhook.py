from skyhook_common import *
import time
import rados

class SkyhookDM:
    def __init__(self):
        self.client = None
        self.addr = None


    def connect(self, ip):
        addr = ip+':8786'
        self.addr = addr
        client = Client(addr)
        self.client = client

    
    def writeDataset(self, path, dstname):
        def runOnDriver(path, dstname, addr):
            import skyhook_driver as sd
            res = sd.writeDataset(path, dstname, addr)
            return res

        fu = self.client.submit(runOnDriver, path, dstname, self.addr)
        result = fu.result()
        return result

    def getDataset(self, name):
        cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
        cluster.connect()
        ioctx = cluster.open_ioctx('hepdatapool')
        size = ioctx.get_xattr(name, "size")
        data = ioctx.read(name, length = int(size))
        ioctx.close()
        cluster.shutdown()
        data = json.loads(data)
        files = []
        for item in data['files']:
            file = File(item['name'], item['file_attributes'], item['file_schema'], name, item['ROOTDirectory'])
            files.append(file)

        dataset = Dataset(data['dataset_name'], data['size'], files)
        return dataset
    
    #Events;1.Jet_puId
    #Events;1.SV_x
    #testdata.nano_tree.root.Events;1.Muon_dzErr
    
    def runQuery(self, obj, querystr):
        if 'File' in str(obj):
            obj_prefix = obj.dataset + '@' + obj.name + '@' + obj.ROOTDirectory

            brs = querystr.split('project')[1].split()[0].split(',')

            commands = []
            for br in brs:
                elems = br.split('.')
                br_name = elems[-1]
                elems.remove(br_name)
                tmp_prefix = ''
                for elem in elems:
                    tmp_prefix = tmp_prefix + '@' + elem
                cmd_prefix = obj_prefix + '@' + tmp_prefix
                data_schema = ''

                f_schema = obj.getSchema()
                found = False

                for i in range(len(elems)):
                    for j in range(len(f_schema['children'])):
                        ch_sche = f_schema['children'][j]
                        if elems[i] == ch_sche['name']:
                            f_schema = ch_sche
                            break
        
                for m in range(len(f_schema['children'])):
                    ch_sche = f_schema['children'][m]
                    if elems[-1] == ch_sche['name']:
                        found = True
                        f_schema = ch_sche
                        break
                
                if found:
                    data_schema = f_schema['data_schema']
                
                print('prefix:' + cmd_prefix)
                print('data_schema:' + data_schema)
                commands.append('')

            # def runOnDriver(objname):
            #     tmppath = '/users/xweichu/projects/pool/'
            #     bf = open(tmppath + objname, 'rb')
            #     reader = pa.ipc.open_stream(bf)
            #     batches = [b for b in reader]
            #     table = pa.Table.from_batches(batches)
            #     return str(table.schema)
            
            # futures = []

            # for objname in objnames:
            #     futures.append(self.client.submit(runOnDriver,objname))

            # tables = self.client.gather(futures)
            
            # return tables
        return 0
    
    def getTreeSchema(self, file, path):
        elems = path.split('.')
        f_schema = file.getSchema()

        i = 2
        found = False

        for i in range(len(elems) - 1):
            for j in range(len(f_schema['children'])):
                ch_sche = f_schema['children'][j]
                if elems[i] == ch_sche['name']:
                    f_schema = ch_sche
                    break
        
        for m in range(len(f_schema['children'])):
            ch_sche = f_schema['children'][m]
            if elems[-1] == ch_sche['name']:
                found = True
                break

        treeSchema = ''
        if found:
            for child in f_schema['children']:
                treeSchema = treeSchema + '; ' + child['node_id'] + ' 3 1 0 ' + child['name']
        print(treeSchema)
        return treeSchema

        




