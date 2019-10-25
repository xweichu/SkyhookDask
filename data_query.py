from skyhook import SkyhookDM
sk = SkyhookDM()
sk.connect('10.10.1.2')
# data = sk.query('dtest', ['--project-cols extendedprice,tax', '--table-name lineitem'])
data = sk.query('dtest', ['project-cols extendedprice,tax', 'limit 2', 'table-name lineitem'])
print data
sk.getSchema('dtest')