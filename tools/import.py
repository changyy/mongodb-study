# -*- coding: utf-8 -*-

#from pymongo import MongoClient
import fileinput
import json
import argparse
parser = argparse.ArgumentParser(description='Mongodb log')
parser.add_argument('files', metavar='File', type=str, nargs='+', help='file with lots of json log split by newline')
parser.add_argument('--host', metavar='DB Host', type=str, help='the ip for database host')
parser.add_argument('--database', metavar='Database', type=str, help='a name for databases')
parser.add_argument('--collection', metavar='Collection', type=str, help='a name for collection (table name for SQL DB)')
parser.add_argument('--deletelog', action='store_true', help='delete the import file')

if __name__ == "__main__":
	args = parser.parse_args()
	if args.database == None:
		args.database = 'db'
	if args.collection == None:
		args.collection = 'test'
	if args.host <> None:
		args.host = str(args.host)

	rec = []
	for f in args.files:
		try:
			if f == '-': # stdin
				f = None
			for line in fileinput.input(f):
				try:
					rec.append(json.loads(line))
				except Exception, e:
					print e	
				pass
			if args.deletelog and f:
				import os
				os.remove(f)
		except Exception, e:
			print e

	print 'Import: ', len(rec)
	if len(rec) > 0:

		from pymongo import MongoClient
		client = MongoClient(args.host)
		database = client[args.database]
		table = database[args.collection]
		print table.insert(rec)
		client.close()
