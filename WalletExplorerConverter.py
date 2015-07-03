import csv, sys, time, boto.dynamodb2

from boto.dynamodb2.table import Table
from boto.dynamodb2.fields import HashKey, RangeKey, GlobalAllIndex
from boto.dynamodb2.types import NUMBER
from time import sleep

# functions
def get_conversion_factor(time):
	# find closest date
	mindate = min(date, key=lambda x:abs(x-time))
	# grab conversion factor
	key = date.index(mindate)
	conversion_factor = float(price[key])
	return conversion_factor

# Load Historical Bitcoin Data
price = []
date = []
with open("price_data/data615.dat", "r") as f:
	reader = csv.reader(f,delimiter=',')
	try:
		for row in reader:
			price.append(float(row[1]))
			date.append(float(row[0]))
	except csv.Error as e:
		sys.exit('file {}, line {}: {}'.format(f, reader.line_num, e))

# Connect to Dynamo.db

conn = boto.dynamodb2.connect_to_region('eu-west-1')

# Create an object of table type that represents WalletExplorer as interpreted through a 
# connection to 'eu-west-1' region by the Table function of the 
# boto.dynamodb2.table module
table = Table('WalletExplorer', connection=conn)

item = table.lookup("BetVIP.com", "000002cedb1e3337733745f23e4c5ccdc31867aaa7da4895fd047782d8808bab")

print "Date: %f USD/BTC: %f" % (item['Transaction Date'], item['balance']) 

dataset = table.scan()

count = 0
# usd_balance = []
with open("usd_balance", 'w') as f:
	for i in dataset:
		print count
		conversion_factor = get_conversion_factor(float(i['Transaction Date']))
		f.write("%f\n" % (conversion_factor * float(i['balance'])))
		count = count + 1

# pseudocode
# Grab date 
# Grab date and match with closest historical date
# On match grab conversion_rate(index)
# usd_balance = conversion_factor multiplied by balance
# Write usd_balance to list
# iterate over all lines.
# write list to file

# Functions needed
# database-query
# date-match
# convert
# write






