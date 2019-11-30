#This is an example of dask server side skyhook_driver library.
#Copy to /usr/lib/python2.7/ so that it can be imported
from dask.distributed import Client
import dask.delayed
import pyarrow as pa
from StringIO import StringIO
from pyarrow import csv
from skyhook_common import *
import rados

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


def writeDataset(path, dstname, addr, dst_type = 'root'):
 
    #internal functions

    def match_skyhook_datatype(d_type):
        field_types = {}
        field_types[None] = 0
        field_types['int8'] = 1
        field_types['int16'] = 2
        field_types['int32'] = 3
        field_types['int64'] = 4
        field_types['uint8'] = 5
        field_types['uint16'] = 6
        field_types['uint32'] = 7
        field_types['uint64'] = 8
        field_types['char'] = 9
        field_types['uchar'] = 10
        field_types['bool'] = 11
        field_types['float'] = 12
        field_types['float64'] = 13
        field_types['date'] = 14
        field_types['string'] = 15
        field_types['[0, inf) -> bool'] = 16
        field_types['[0, inf) -> char'] = 17
        field_types['[0, inf) -> uchar'] = 18
        field_types['[0, inf) -> int8'] = 19
        field_types['[0, inf) -> int16'] = 20
        field_types['[0, inf) -> int32'] = 21
        field_types['[0, inf) -> int64'] = 22
        field_types['[0, inf) -> uint8'] = 23
        field_types['[0, inf) -> uint16'] = 24
        field_types['[0, inf) -> uint32'] = 25
        field_types['[0, inf) -> uint64'] = 26
        field_types['[0, inf) -> float'] = 27
        field_types['[0, inf) -> float64'] = 28

        # print(d_type)
        # no float64
        # [0, inf) doesn't work? 

        if str(d_type) in field_types.keys():
            return field_types[str(d_type)]

        return 0


    def buildObj(dst_name, branch, subnode, obj_id):
        # change the name here to object_id which is the node_id
        # objname = branch.name.decode("utf-8")
        objname = str(obj_id)
        parent = subnode.parent
        while parent is not None:
            objname = parent.name + '@' + objname
            parent = parent.parent
        
        objname = dst_name + '@' + objname

        # this is for the event id colo
        event_id_col = pa.field('Event_ID', pa.int64())
        id_array = range(branch.numentries)

        
        field = None
        fieldmeta = {}
        fieldmeta['BasketSeek'] = bytes(branch._fBasketSeek)
        fieldmeta['BasketBytes'] = bytes(branch._fBasketBytes)
        fieldmeta['Compression'] = bytes(str(branch.compression))
        fieldmeta['Compressionratio'] = bytes(str(branch.compressionratio()))
        
        if('inf' not in str(branch.interpretation.type) and 'bool' in str(branch.interpretation.type)):
            function=getattr(pa,'bool_')
            field = pa.field(branch.name, function(), metadata = fieldmeta)
            
        elif('inf' in str(branch.interpretation.type)):
            function= getattr(pa,'list_')
            subfunc = None
            if('bool' in str(branch.interpretation.type)):
                subfunc = getattr(pa,'bool_')
            else:
                subfunc = getattr(pa,str(branch.interpretation.type).split()[-1])
            field = pa.field(branch.name, function(subfunc()), metadata = fieldmeta)
            
        else:
            function=getattr(pa,str(branch.interpretation.type))
            field = pa.field(branch.name, function(), metadata = fieldmeta)
            
        schema = pa.schema([event_id_col, field])
        
        #metadata for the arrow table
        sche_meta = {}
        #versions
        sche_meta['0'] = bytes(0)
        sche_meta['1'] = bytes(0)
        sche_meta['2'] = bytes(0)
        #data format -> arrow
        sche_meta['3'] = bytes(5)
        sche_meta['4'] = bytes(str(obj_id) + ' ' + str(match_skyhook_datatype(branch.interpretation.type)) + ' 0 1 ' + str(branch.name))
        sche_meta['5'] = bytes('n/a')
        sche_meta['6'] = bytes(str(branch.name.decode("utf-8")))
        sche_meta['7'] = bytes(branch.numentries)

        schema = schema.with_metadata(sche_meta)
        table = pa.Table.from_arrays([id_array, branch.array().tolist()],schema = schema)
        
        #Serialize arrow table to bytes
        batches = table.to_batches()
        sink = pa.BufferOutputStream()
        writer = pa.RecordBatchStreamWriter(sink, schema)
        for batch in batches:
            writer.write_batch(batch)
        buff = sink.getvalue()
        buff_bytes = buff.to_pybytes()
        
        #data should be written into the ceph pools
        #for now writ the data into 'data' which is a local folder
        # cephobj = open('/users/xweichu/projects/pool/'+objname,'wb+')

        cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
        cluster.connect()
        ioctx = cluster.open_ioctx('hepdatapool')
        ioctx.write_full(objname, buff_bytes)
        ioctx.close()
        cluster.shutdown()

        # cephobj.write(buff_bytes)
        # cephobj.close()

    def growTree(dst_name, node, rootobj):
        
        if 'allkeys' not in dir(rootobj):
            return

        child_id = 0

        data_schema = None
        if ('TTree' in str(node.classtype)):
            data_schema = ''

        for key in rootobj.allkeys():

            datatype = None
            if 'Branch' in str(type(rootobj[key])):
                datatype = str(rootobj[key].interpretation.type)
                
            subnode = RootNode(key.decode("utf-8"),str(type(rootobj[key])).split('.')[-1].split('\'')[0], datatype, node, child_id, None)
            node.children.append(subnode)
            growTree(dst_name, subnode, rootobj[key])
            
            #build the object if it's a branch
            if('Branch' in str(subnode.classtype)):
                buildObj(dst_name, rootobj[key], subnode, child_id)
                data_schema = data_schema + str(child_id) + ' ' + str(match_skyhook_datatype(subnode.datatype)) + ' 0 1 ' + subnode.name + '; '

            child_id += 1

        node.data_schema = data_schema            
        

    def tree_traversal(root):
        output = {}
        if root!=None:   
            children = root.children
        output["name"] = str(root.name)
        output["classtype"] = str(root.classtype)
        output['datatype'] = str(root.datatype)
        output['node_id'] = str(root.node_id)
        output['data_schema'] = root.data_schema
        output["children"] = []

        for node in children:
            output["children"].append(tree_traversal(node))
        return output

    def process_file(path):
        root_dir = uproot.open(path)
        #build objects and generate json file which dipicts the logical structure
        tree = RootNode(root_dir.name.decode("utf-8"), str(type(root_dir)).split('.')[-1].split('\'')[0], None, None, 0, None)
        growTree(dstname + '@' + path.split('/')[-1], tree, root_dir)
        logic_schema = tree_traversal(tree)
        return logic_schema

    client = Client(addr)
    #for now, the path is a local path on the driver server
    #suppose the file has been downlaoded
    #get the list of files in the path location and is the dst_type
    file_list = [f for f in listdir(path) if isfile(join(path, f)) and dst_type in f]
    
    #dataset name is the last dir name, it can be -2
    # dstname = dstname
    
    #the metadata object should is a json file which has the following content so far:
    #1.the dataset name, 2.a list of files
    #the metadata for each file should include: file attribute json string, and root file structure json string. 
    metadata = {}
    metadata['dataset_name'] = dstname
    metadata['files'] = []
    
    #the size of the dataset
    total_size = 0
    
    #the concept of dataset can have more attributes, to be added here
    
    #process each file in the file list
    futures = []
    for r_file in file_list:
        file_meta = {}
        file_meta['name'] = r_file
        file_meta['ROOTDirectory'] = uproot.open(join(path, r_file)).name
        #read the file attributes based on the stat() info
        stat_res = os.stat(join(path, r_file))
        stat_res_dict = {}
        stat_res_dict['size'] = stat_res.st_size
        total_size += stat_res.st_size
        stat_res_dict['last access time'] = stat_res.st_atime
        stat_res_dict['last modified time'] = stat_res.st_mtime
        stat_res_dict['last changed time'] = stat_res.st_ctime
        #stat_json = json.dumps(stat_res_dict)
        file_meta['file_attributes'] = stat_res_dict
        
        future = client.submit(process_file, join(path, r_file))

        #logic_struct_json = json.dumps(logic_struct)
        file_meta['file_schema'] = 'future'
        futures.append(future)
        
        metadata['files'].append(file_meta)
    
    schemas = client.gather(futures)

    i = 0
    for item in metadata['files']:
        item['file_schema'] = schemas[i]
        i += 1
    
    metadata['size'] = total_size
    
    #constructed the metadata object
    #json formatter can be used to view the content of the json file clearly
    #https://jsonformatter.curiousconcept.com/
    cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
    cluster.connect()
    ioctx = cluster.open_ioctx('hepdatapool')
    ioctx.write_full(dstname, json.dumps(metadata))
    ioctx.close()
    cluster.shutdown()
    # with open('/users/xweichu/projects/pool/' + dstname, 'w') as outfile:
    #     json.dump(metadata, outfile)
    return True


# def getDataset(name):
#     data = json.loads(open('/users/xweichu/projects/pool/data.json').read())
#     files = []
#     for item in data['files']:
#         file = File(item['name'], item['file_attributes'], item['file_schema'], name)
#         files.append(file)

#     dataset = Dataset(data['dataset_name'], data['size'], files)
#     return dataset

