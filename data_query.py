from skyhook import SkyhookDM
sk = SkyhookDM()
sk.connect('128.105.144.19')
# data = sk.query('dtest', ['--project-cols extendedprice,tax', '--table-name lineitem'])
data = sk.query('dtest', ['project-cols extendedprice,tax', 'limit 2', 'table-name lineitem'])
print data
sk.getSchema('dtest')

import skyhook_driver as sd
sd.writeDataset('/users/xweichu/projects/testdata')