from skyhook import Skyhook
sk = Skyhook('10.10.1.2')
data = sk.query('dtest', ['--project-cols extendedprice,tax', '--table-name lineitem'])
print data