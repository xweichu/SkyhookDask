#This is an example of dask server side skyhook_driver library.
#Copy to /usr/lib/python2.7/ so that it can be imported anywhere 
from skyhook_common import *

def writeDataset(file_urls, dstname, addr, dst_type = 'root'):
 
    #internal functions
    def match_skyhook_datatype(d_type):
        #how to handle float32
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
        field_types['[0, inf) -> float32'] = 27
        field_types['[0, inf) -> float64'] = 28

        if str(d_type) in field_types.keys():
            return field_types[str(d_type)]

        return 0

    def buildObj(dst_name, branch, subnode, obj_id):
        from collections import OrderedDict 
        # change the name here to object_id which is the node_id
        # objname = branch.name.decode("utf-8")
        objname = '.' + str(obj_id)
        parent = subnode.parent
        while parent is not None:
            objname = parent.name + '#' + objname
            parent = parent.parent
        
        objname = dst_name + '#' + objname

        # this is for the event id colo
        event_id_col = pa.field('EVENT_ID', pa.int64())
        id_array = range(branch.numentries)

        
        field = None
        fieldmeta = {}
        fieldmeta['BasketSeek'] = bytes(branch._fBasketSeek)
        fieldmeta['BasketBytes'] = bytes(branch._fBasketBytes)
        fieldmeta['Compression'] = bytes(str(branch.compression))
        fieldmeta['Compressionratio'] = bytes(str(branch.compressionratio()))
        
        if('inf' not in str(branch.interpretation.type) and 'bool' in str(branch.interpretation.type)):
            function=getattr(pa,'bool_')
            field = pa.field(branch.name.upper(), function(), metadata = fieldmeta)
            
        elif('inf' in str(branch.interpretation.type)):
            function= getattr(pa,'list_')
            subfunc = None
            if('bool' in str(branch.interpretation.type)):
                subfunc = getattr(pa,'bool_')
            else:
                subfunc = getattr(pa,str(branch.interpretation.type).split()[-1])
            field = pa.field(branch.name.upper(), function(subfunc()), metadata = fieldmeta)
            
        else:
            try:
                function=getattr(pa,str(branch.interpretation.type))
                field = pa.field(branch.name.upper(), function(), metadata = fieldmeta)
            except Exception,e:
                print(str(e))
                field = pa.field(branch.name.upper(), pa.float64(), metadata = fieldmeta)
                
            
        schema = pa.schema([event_id_col, field])
        
        #metadata for the arrow table
        # ordered dictionary
        # sche_meta = OrderedDict()
        #norman dictionary

        sche_meta = {}

        #versions
        # sche_meta['skyhook_version'] = bytes(0)
        # sche_meta['data_schema_version'] = bytes(0)
        # sche_meta['data_structure_version'] = bytes(0)
        # #data format -> arrow
        # sche_meta['data_format_type'] = bytes(11)
        sche_meta['0'] = bytes('0 4 0 0 EVENT_ID;' + str(obj_id) + ' ' + str(match_skyhook_datatype(branch.interpretation.type)) + ' 0 1 ' + str(branch.name).upper())
        # sche_meta['db_schema'] = bytes('n/a')
        # sche_meta['table_name'] = bytes(str(branch.name.decode("utf-8")))
        # sche_meta['num_rows'] = bytes(int(branch.numentries))

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

        
        size_limit = 238000000
        
        num_partitions = 1

        if len(buff_bytes) > size_limit:
            total_rows = len(table.columns[0])
            num_partitions = len(buff_bytes)/size_limit
            num_partitions += 1
            batch_size = total_rows/num_partitions
            batches = table.to_batches(batch_size)
        
        
        #data should be written into the ceph pools
        #for now writ the data into 'data' which is a local folder
        try:
            if num_partitions == 1:
            # Write to the Ceph pool
                cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
                cluster.connect()
                ioctx = cluster.open_ioctx('hepdatapool')
                ioctx.write_full(objname + '.0', buff_bytes)
                ioctx.set_xattr(objname + '.0', 'size', str(len(buff_bytes)))
                ioctx.close()
                cluster.shutdown()
            else:
                cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
                cluster.connect()
                ioctx = cluster.open_ioctx('hepdatapool')
                i = 0
                for batch in batches:
                    sink = pa.BufferOutputStream()
                    writer = pa.RecordBatchStreamWriter(sink, schema)
                    writer.write_batch(batch)
                    buff = sink.getvalue()
                    buff_bytes = buff.to_pybytes()
                    ioctx.write_full(objname + '.' + str(i), buff_bytes)
                    ioctx.set_xattr(objname + '.' + str(i), 'size', str(len(buff_bytes)))
                    i += 1
                ioctx.close()
                cluster.shutdown()



        except Exception,e:
            print(str(e))
            # print(str(len(buff_bytes)))
            # print("number of batches:" + str(len(batches)))
            # sub = pa.Table.from_batches([batches[1]])
            # print(sub.schema)
            # import zlib
            # compressed_data = zlib.compress(buff_bytes)
            # print("compressed:" + str(len(compressed_data)))

        #writ it to local folder 
        # cephobj = open('/users/xweichu/projects/pool/'+objname,'wb+')
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
                try:
                    buildObj(dst_name, rootobj[key], subnode, child_id)
                except Exception,e:
                    print(str(e))
                    print(subnode)

                subnode.data_schema = str(child_id) + ' ' + str(match_skyhook_datatype(subnode.datatype)) + ' 0 1 ' + subnode.name
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

    def process_file(url):
        import wget
        filename = wget.download(url)
        root_dir = uproot.open(filename)

        stat_res = os.stat(filename)
        stat_res_dict = {}
        stat_res_dict['size'] = stat_res.st_size
        stat_res_dict['last access time'] = stat_res.st_atime
        stat_res_dict['last modified time'] = stat_res.st_mtime
        stat_res_dict['last changed time'] = stat_res.st_ctime
        stat_res_dict['name'] = filename

        #stat_json = json.dumps(stat_res_dict)
        #build objects and generate json file which dipicts the logical structure
        tree = RootNode(root_dir.name.decode("utf-8"), str(type(root_dir)).split('.')[-1].split('\'')[0], None, None, 0, None)
        growTree(dstname + '#' + filename, tree, root_dir)
        logic_schema = tree_traversal(tree)
        os.remove(filename)
        res = [stat_res_dict, logic_schema]
        return res

    client = Client(addr)
    #for now, the path is a local path on the driver server
    #suppose the file has been downlaoded
    #get the list of files in the path location and is the dst_type
    file_list = file_urls
    
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
        file_meta['ROOTDirectory'] = uproot.open(r_file).name
        #read the file attributes based on the stat() info
        # stat_res = os.stat(join(path, r_file))
        # stat_res_dict = {}
        # stat_res_dict['size'] = stat_res.st_size
        # total_size += stat_res.st_size
        # stat_res_dict['last access time'] = stat_res.st_atime
        # stat_res_dict['last modified time'] = stat_res.st_mtime
        # stat_res_dict['last changed time'] = stat_res.st_ctime
        # #stat_json = json.dumps(stat_res_dict)
        # file_meta['file_attributes'] = stat_res_dict
        
        future = client.submit(process_file, r_file)

        #logic_struct_json = json.dumps(logic_struct)
        # file_meta['file_schema'] = 'future'
        futures.append(future)
        
        metadata['files'].append(file_meta)
    
    res = client.gather(futures)

    i = 0
    for item in metadata['files']:
        item['file_schema'] = res[i][1]
        item['file_attributes'] = res[i][0]
        item['name'] = res[i][0]['name']
        total_size += int(res[i][0]['size'])
        i += 1
    
    metadata['size'] = total_size
    
    #constructed the metadata object
    #json formatter can be used to view the content of the json file clearly
    #https://jsonformatter.curiousconcept.com/
    cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
    cluster.connect()
    ioctx = cluster.open_ioctx('hepdatapool')
    output = json.dumps(metadata,indent=4)
    ioctx.write_full(dstname, output)
    ioctx.set_xattr(dstname, 'size', str(len(output)))
    ioctx.close()
    cluster.shutdown()
    # with open('/users/xweichu/projects/pool/' + dstname, 'w') as outfile:
    #     json.dump(metadata, outfile)
    return True