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
        data = json.loads(open('data.json').read())
        files = []
        for item in data['files']:
            file = File(item['name'], item['file_attributes'], item['file_schema'], name)
            files.append(file)

        dataset = Dataset(data['dataset_name'], data['size'], files)
        return dataset
    
    def buildObj(self, dst_name, branch, subnode):
        objname = branch.name.decode("utf-8")
        
        parent = subnode.parent
        while parent is not None:
            objname = parent.name + '.' + objname
            parent = parent.parent
            
        objname = dst_name + '.' + objname
        
        field = None
        fieldmeta = {}
        fieldmeta['BasketSeek'] = bytes(branch._fBasketSeek)
        fieldmeta['BasketBytes'] = bytes(branch._fBasketBytes)
        fieldmeta['Compression'] = bytes(str(branch.compression), 'utf8')
        fieldmeta['Compressionratio'] = bytes(str(branch.compressionratio()),'utf8')
        
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
            
        schema = pa.schema([field])
        
        #metadata for the arrow table
        sche_meta = {}
        #versions
        sche_meta['0'] = bytes(0)
        sche_meta['1'] = bytes(0)
        sche_meta['2'] = bytes(0)
        #data format -> arrow
        sche_meta['3'] = bytes(5)
        sche_meta['4'] = bytes('0' + ' ' + str(field.type) + ' 0 1 ' + str(branch.name), 'utf8')
        sche_meta['5'] = bytes('n/a','utf8')
        sche_meta['6'] = bytes(str(subnode.parent.name),'utf8')
        sche_meta['7'] = bytes(branch.numentries)

        schema.with_metadata(sche_meta)
        table = pa.Table.from_arrays([branch.array().tolist()],schema = schema)
        
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
        cephobj = open('./data/'+objname,'wb+')
        cephobj.write(buff_bytes)
        cephobj.close()
    

    def growTree(self, dst_name, node, rootobj):
        
        if 'allkeys' not in dir(rootobj):
            return
        
        for key in rootobj.allkeys():
            datatype = None
            if 'Branch' in str(type(rootobj[key])):
                datatype = str(rootobj[key].interpretation.type)
                
            subnode = RootNode(key.decode("utf-8"),str(type(rootobj[key])).split('.')[-1].split('\'')[0], datatype, node)
            node.children.append(subnode)
            self.growTree(dst_name, subnode, rootobj[key])
            
            #build the object if it's a branch
            if('Branch' in str(subnode.classtype)):
                self.buildObj(dst_name, rootobj[key], subnode)


    def tree_traversal(self, root):
        output = {}
        if root!=None:   
            children = root.children
        output["name"] = str(root.name)
        output["classtype"] = str(root.classtype)
        output['datatype'] = str(root.datatype)
        output["children"] = []

        for node in children:
            output["children"].append(self.tree_traversal(node))
        return output