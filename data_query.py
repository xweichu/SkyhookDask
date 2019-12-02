from skyhook import SkyhookDM
sk = SkyhookDM()
sk.connect('128.105.144.19')
# data = sk.query('dtest', ['--project-cols extendedprice,tax', '--table-name lineitem'])
data = sk.query('dtest', ['project-cols extendedprice,tax', 'limit 2', 'table-name lineitem'])
print data
sk.getSchema('dtest')

import skyhook_driver as sd
sd.writeDataset('/users/xweichu/projects/testdata')

from skyhook import SkyhookDM
sk = SkyhookDM()
sk.connect('128.105.144.211')
sk.writeDataset('/users/xweichu/projects/testdata', 'sample_dataset')
dst = sk.getDataset('sample_dataset')
dst.getFiles()
f = dst.getFiles()[0]
sk.runQuery(f,'select event>X, project Events;1.Muon_dzErr,Events;1.SV_x,Events;1.Jet_puId')
f.getSchema()
sk.getTreeSchema(f,'a.b.Events;1.Muon_dzErr')

import pyarrow as pa
buf = open('sample_dataset@nano_dy.root@nano_tree.root@Events;1@388', 'rb')
reader = pa.ipc.open_stream(buf)
batches = [b for b in reader]
tb = pa.Table.from_batches(batches)


import pyarrow as pa
schema = pa.schema([pa.field('Event_ID', pa.int64())])
meta = {}
meta['test'] = 'testval'
schema.add_metadata(meta)
schema.metadata


import rados
cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
cluster.connect()
cluster_stats = cluster.get_cluster_stats()

pools = cluster.list_pools()

ioctx = cluster.open_ioctx('hepdatapool')
ioctx.write_full("hw", "Hello World!")
ioctx.read('hw')
ioctx.remove_object("hw")


cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
cluster.connect()
ioctx = cluster.open_ioctx('hepdatapool')
ioctx.write_full(dstname, json.dumps(metadata))
ioctx.close()
cluster.shutdown()


rados -p hepdatapool ls

cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
cluster.connect()
ioctx = cluster.open_ioctx('hepdatapool')
data = ioctx.read('sample_dataset')
ioctx.close()
cluster.shutdown()
data = json.loads(data)

from skyhook import SkyhookDM
sk = SkyhookDM()
sk.connect('128.105.144.228')
sk.writeDataset('/users/xweichu/projects/testdata', 'testdst')

batches = table.to_batches()
sink = pa.BufferOutputStream()
writer = pa.RecordBatchStreamWriter(sink, schema)
for batch in batches:
    writer.write_batch(batch)
buff = sink.getvalue()
buff_bytes = buff.to_pybytes()

# file location:http://uaf-1.t2.ucsd.edu/jeff_data/

from skyhook import SkyhookDM
sk = SkyhookDM()
sk.connect('128.105.144.211')
dst = sk.getDataset('dst')
dst.getFiles()
f = dst.getFiles()[0]
sk.runQuery(f,'select event>X, project Events;1.Muon_dzErr,Events;1.SV_x,Events;1.Jet_puId')
sk.runQuery(dst,'select event>X, project Events;1.Muon_dzErr,Events;1.SV_x,Events;1.Jet_puId')

import pyarrow as pa
buf = open('sample_dataset@nano_dy.root@nano_tree.root@Events;1@683', 'rb')
reader = pa.ipc.open_stream(buf)
batches = [b for b in reader]
tb1 = pa.Table.from_batches(batches)

buf = open('sample_dataset@nano_dy.root@nano_tree.root@Events;1@688', 'rb')
reader = pa.ipc.open_stream(buf)
batches = [b for b in reader]
tb2 = pa.Table.from_batches(batches)

buf = open('sample_dataset@nano_dy.root@nano_tree.root@Events;1@375', 'rb')
reader = pa.ipc.open_stream(buf)
batches = [b for b in reader]
tb3 = pa.Table.from_batches(batches)

tb = tb1.append_column(tb2.field(1), tb2.columns[1])
tb = tb.append_column(tb3.field(1), tb3.columns[1])

    #Events;1.Jet_puId
    #Events;1.SV_x
    #testdata.nano_tree.root.Events;1.Muon_dzErr

# sample command
# bin/run-query    
# --wthreads 1 
# --qdepth 10 
# --query hep
# --pool testpool 
# --data-schema "0 3 0 0 col1;"
# --project-cols "col1"   // optional
# --select-preds "col1,gt,5;eventid,geq0;eventid,lt,5" //optional
# --num-objs 5
# --start-obj 0
# --output-format "SFT_PYARROW_BINARY"
# --oid-prefix "dataset.tree.tree"  // this will generate 5 objs numbered dataset.tree.tree.0,1,2,3,4


# bin/run-query --wthreads 1 --qdepth 10 --query hep --pool testpool --start-obj 0 --output-format "SFT_PYARROW_BINARY" --data-schema "0 3 0 0 col1;" --project-cols "col1" --num-objs 5 --oid-prefix "dataset.tree.tree"


# /mnt/sda4/skyhookdm-ceph/build/bin/run-query --num-objs 1 --pool hep --wthreads 1 --qdepth 10   --output-format SFT_PYARROW_BINARY --query hep --use-cls --data-schema "0 4 0 0  EVENT_ID;388 11 0 1 HLT_AK8PFHT900_TrimMass50;" --project-cols "event_id,HLT_AK8PFHT900_TrimMass50"

#list objs : rados --pool hepdatapool ls -

import rados
cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
cluster.connect()
ioctx = cluster.open_ioctx('hepdatapool')
name = 'testdst#nano_dy.root#nano_tree.root#Events;1#.388'
size = ioctx.get_xattr(name, "size")
data = ioctx.read(name, length = int(size))
import pyarrow as pa
buf = data
reader = pa.ipc.open_stream(buf)
batches = [b for b in reader]
tb1 = pa.Table.from_batches(batches)




prog = '/mnt/sda4/skyhookdm-ceph/build/bin/run-query '
command = '--num-objs 1 --pool jeff --wthreads 1 --qdepth 10   --output-format SFT_PYARROW_BINARY --query hep --use-cls --data-schema \"0 4 0 0  EVENT_ID;388 11 0 1 HLT_AK8PFHT900_TrimMass50;\" --project-cols \"event_id,HLT_AK8PFHT900_TrimMass50\"'
command = '--num-objs 1 --pool jeff --wthreads 1 --qdepth 10   --output-format SFT_PYARROW_BINARY --query hep --use-cls --data-schema \"0 4 0 0  EVENT_ID;388 11 0 1 HLT_AK8PFHT900_TrimMass50;\" --project-cols \"event_id,HLT_AK8PFHT900_TrimMass50\"'
import os
result = os.popen(prog + command).read()


from skyhook import SkyhookDM
sk = SkyhookDM()
sk.connect('128.105.144.228')
dst = sk.getDataset('testdst')
dst.getFiles()
f = dst.getFiles()[0]
sk.runQuery(f,'select event>X, project Events;1.HLT_AK8PFHT900_TrimMass50,Events;1.HLT_AK8PFHT900_TrimMass50,Events;1.HLT_AK8PFHT900_TrimMass50')