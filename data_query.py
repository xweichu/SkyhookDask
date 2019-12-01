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
sk.connect('128.105.144.211')
sk.writeDataset('/users/xweichu/projects/dst', 'testdst')

batches = table.to_batches()
sink = pa.BufferOutputStream()
writer = pa.RecordBatchStreamWriter(sink, schema)
for batch in batches:
    writer.write_batch(batch)
buff = sink.getvalue()
buff_bytes = buff.to_pybytes()

# file location:http://uaf-1.t2.ucsd.edu/jeff_data/