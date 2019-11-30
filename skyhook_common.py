import os
import json
import uproot
import dask.delayed
import pyarrow as pa
from pyarrow import csv
from os import listdir
from os.path import isfile, join
from dask.distributed import Client
import rados


class Dataset:
    def __init__(self, name, size, files):
        self.name  = name
        self.size = size
        self.files = files
        
    def getFiles(self):
        return self.files
    
    def getSize(self):
        return self.size
    
    def runQuery(self, querystr):
        print(querystr)
        
    def __str__(self):
        return '\"Dataset: ' + self.name + ', ' + str(self.size) + ' bytes\"'
    
    def __repr__(self):
        return '\"Dataset: ' + self.name + ', ' + str(self.size) + ' bytes\"'


class File:
    def __init__(self, name, attributes, schema, dataset, rootdirectory):
        self.name = name
        self.attributes = attributes
        self.schema = schema
        self.dataset = dataset
        self.ROOTDirectory = rootdirectory
        
    def getAttributes(self):
        return self.attributes
    

    def getRoot(self):
        node = self.buildTree(self.schema, None)
        return node
    
    def getSchema(self):
        return self.schema
    
    
    def buildTree(self, nd_dict, parent):
        node = RootNode(nd_dict['name'], nd_dict['classtype'], nd_dict['datatype'], parent, nd_dict['node_id'], nd_dict['data_schema'])
        node.children = []
        for item in nd_dict['children']:
            tmp = self.buildTree(item, node)
            node.children.append(tmp)
        return node
    
    def runQuery(self, querystr):
        obj_prefix = self.dataset + '.' + self.name
        print(obj_prefix)
        
    def __str__(self):
        return '\"File: ' + self.name + ', ' + str(self.attributes['size']) + ' bytes\"'
    
    def __repr__(self):
        return '\"File: ' + self.name + ', ' + str(self.attributes['size']) + ' bytes\"'


class RootNode(object):
    def __init__(self, name, classtype, datatype, parent, node_id, data_schema):
        self.children  = []
        self.name = name
        self.classtype = classtype
        self.parent = parent
        self.datatype = datatype
        self.node_id = node_id
        self.data_schema = data_schema
        
    def getName(self):
        return self.name
    
    def getClassType(self):
        return self.classtype
    
    def getDataType(self):
        return self.datatype
    
    def getChildren(self):
        #for child in self.children:
            #print(child.classtype + ': ' + child.name + ', ' + child.datatype)
        return self.children
    
    def getParent(self):
        #print(self.parent.classtype + ': ' + self.parent.name + ', ' + self.parent.datatype)
        return self.parent
    
    def __str__(self):
        return '\"RootNode: ' + self.classtype + ': ' + self.name + ', ' + self.datatype +'\"'
    
    def __repr__(self):
        return '\"RootNode: ' + self.classtype + ': ' + self.name + ', ' + self.datatype +'\"'

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

        if str(d_type) in field_types.keys():
            return field_types[str(d_type)]

        return 0