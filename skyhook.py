from skyhook_common import *


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
    
    def runQuery(self, obj, querystr):
        command_template = '--wthreads 1 --qdepth 10 --query hep --pool hepdatapool --start-obj 0 --output-format \"SFT_PYARROW_BINARY\" --data-schema \"#dataschema\" --project-cols \"#colname\" --num-objs #objnum --oid-prefix \"#prefix\"'
        commands = []

        def generateQueryCommand(file, querystr):
            prefix = file.dataset + '@' + file.name + '@' +file.ROOTDirectory
            brs = querystr.split('project')[-1].split()[0].split(',')
            obj_num = 0

            for br in brs:
                elems = br.split('.')
                br_name = elems[-1]
                elems.remove(br_name)
                local_prefix = ''
                for elem in elems:
                    local_prefix = local_prefix + '@' + elem
                obj_prefix = prefix + '@' + local_prefix
                data_schema = ''

                f_schema = file.getSchema()
                found = False

                for i in range(len(elems)):
                    for j in range(len(f_schema['children'])):
                        ch_sche = f_schema['children'][j]
                        if elems[i] == ch_sche['name']:
                            f_schema = ch_sche
                            break
                
                obj_num = len(f_schema['children'])

                for m in range(len(f_schema['children'])):
                    ch_sche = f_schema['children'][m]
                    if br_name == ch_sche['name']:
                        found = True
                        f_schema = ch_sche
                        break
                
                if found:
                    cmd = command_template
                    data_schema = '0 4 0 0 EVENT_ID;' + f_schema['data_schema'].upper() + ';'
                    cmd = cmd.replace('#dataschema', data_schema)
                    cmd = cmd.replace('#colname', br_name.upper())
                    cmd = cmd.replace('#prefix', obj_prefix)
                    cmd = cmd.replace('#objnum', str(obj_num))
                    commands.append(cmd)
                
        if 'File' in str(obj):
            generateQueryCommand(obj, querystr)
        
        if 'Dataset' in str(obj):

            rtfiles = obj.getFiles()

            for rtfile in rtfiles:
                generateQueryCommand(rtfile, querystr)
        
        def exeQuery(command):
            prog = '/mnt/sda4/skyhookdm-ceph/build/bin/run-query '
            import os
            import time
            time.sleep(3)
            # result = os.popen(prog + command).read()
            result = prog + command
            return result

        futures = []
        for command in commands:
            future = self.client.submit(exeQuery, command)
            futures.append(future)
        
        tablestreams = self.client.gather(futures)

        # res = self._mergeTables(tablestreams)

        print(tablestreams)

        return 0
    
    def _mergeTables(self, tablestreams):
        table = None
        for tablestream in tablestreams:
            reader = pa.ipc.open_stream(tablestream)
            batches = [b for b in reader]
            tb = pa.Table.from_batches(batches)
            if table == None:
                table = tb
            else:
                table = table.append_column(tb.field(1), tb.columns[1])

        return table

        




