from skyhook_common import *
import struct


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
        #limit just to 1 obj
        command_template = '--wthreads 1 --qdepth 120 --query hep --pool hepdatapool --start-obj #startobj --output-format \"SFT_PYARROW_BINARY\" --data-schema \"#dataschema\" --project-cols \"#colname\" --num-objs #objnum --oid-prefix \"#prefix\" --subpartitions 10'


        def generateQueryCommand(file, querystr):
            cmds = []
            prefix = file.dataset + '#' + file.name + '#' +file.ROOTDirectory
            brs = querystr.split('project')[-1].split()[0].split(',')
            obj_num = 0

            for br in brs:
                elems = br.split('.')
                br_name = elems[-1]
                elems.remove(br_name)
                local_prefix = ''
                for elem in elems:
                    local_prefix = local_prefix + '#' + elem
                obj_prefix = prefix + local_prefix + '#'
                data_schema = ''
                
                # print(obj_prefix)

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
                        #limit the obj num to 1
                        obj_num = m
                        break
                
                if found:
                    cmd = command_template
                    data_schema = '0 4 0 0 EVENT_ID;' + f_schema['data_schema'] + ';'
                    cmd = cmd.replace('#dataschema', data_schema)
                    cmd = cmd.replace('#colname', 'event_id,'+ br_name)
                    cmd = cmd.replace('#prefix', obj_prefix)
                    #limit the obj num to 1
                    cmd = cmd.replace('#objnum', str(1))
                    cmd = cmd.replace('#startobj', str(obj_num))
                    cmds.append(cmd)
            return cmds
        


        def exeQuery(command):
            prog = '/mnt/sda4/skyhookdm-ceph/build/bin/run-query '
            import os
            result = os.popen(prog + command).read()
            return result

                
        if 'File' in str(obj):
            cmds = generateQueryCommand(obj, querystr)
            futures = []
            for command in cmds:
                future = self.client.submit(exeQuery, command)
                futures.append(future)

            tablestreams = self.client.gather(futures)
            tables = []

            for tablestream in tablestreams:
                sizebf = None
                stream_length = len(tablestream)
                cursor = 0
                batches = []

                while True:
                    if cursor == stream_length:
                        break
                    sizebf = tablestream[cursor:cursor+4]
                    cursor = cursor + 4
                    size = struct.unpack("<i",sizebf)[0]
                    stream = tablestream[cursor:cursor+size]
                    cursor = cursor + size
                    reader = pa.ipc.open_stream(stream)
                    for b in reader:
                        batches.append(b)
                
                #sort batches
                def index(batch1, batch2):
                    tb1 = pa.Table.from_batches([batch1.slice(0,1)])
                    tb2 = pa.Table.from_batches([batch2.slice(0,1)])
                    return tb1.columns[0][0].as_py() > tb2.columns[0][0].as_py()
                
                batches = sorted(batches,index)

                table = pa.Table.from_batches(batches)
                tables.append(table)

            res = tables
            return res
        
        if 'Dataset' in str(obj):

            rtfiles = obj.getFiles()
            tables = []
            futures_set = []

            for rtfile in rtfiles:
                cmds = generateQueryCommand(rtfile, querystr)
                futures = []
                for command in cmds:
                    future = self.client.submit(exeQuery, command)
                    futures.append(future)
                futures_set.append(futures)

            for futures in futures_set:
                tablestreams = self.client.gather(futures)
                res = self._mergeTables(tablestreams)
                tables.append(res)
            
            return tables

        


        return None
    
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
    
    def _extendTables(self, tablestreams):
        table = None
        batches = []
        for tablestream in tablestreams:
            reader = pa.ipc.open_stream(tablestream)
            for b in reader:
                batches.append(b)
        
        table = pa.Table.from_batches(batches)

        return table

        




