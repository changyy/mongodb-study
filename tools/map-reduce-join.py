# -*- coding: utf-8 -*-
import sys
import argparse
parser = argparse.ArgumentParser(description='Mongodb log')
parser.add_argument('--host', metavar='DB Host', type=str, help='the ip for database host')
parser.add_argument('--database', metavar='Collections(SQL:Database)', type=str, help='a name for database(database name for SQL DB)')

parser.add_argument('--collection-1', metavar='Collection(SQL:Table)', type=str, help='a name for collection (table name for SQL DB)')
parser.add_argument('--join-key-1', metavar='Table Join key', nargs='+', type=str, help='a name for collection (table name for SQL DB)')
parser.add_argument('--select-out-1', metavar='Field seletecd out', nargs='+', type=str, help='a name for collection (table name for SQL DB)')

parser.add_argument('--collection-2', metavar='Collection(SQL:Table)', type=str, help='a name for collection (table name for SQL DB)')
parser.add_argument('--join-key-2', metavar='Join key', type=str, nargs='+', help='a name for collection (table name for SQL DB)')
parser.add_argument('--select-out-2', metavar='Field seletecd out', nargs='+', type=str, help='a name for collection (table name for SQL DB)')

parser.add_argument('--result', metavar='Collection(SQL:Table)', help='a collection for result')
parser.add_argument('--show-result', action="store_true", help='print the result')
parser.add_argument('--reset-result', action="store_true", help='reset result before job')
parser.add_argument('--delete-result', action="store_true", help='delete result after job done')

if __name__ == "__main__":
	args = parser.parse_args()
	if args.database == None:
		args.database = 'db'
	if args.host <> None:
		args.host = str(args.host)

	if args.collection_1 == None:
		args.collection_1 = 't1'
	if args.join_key_1 == None:
		args.join_key_1 = ['_id']
	if args.select_out_1 == None:
		args.select_out_1 = ['data']
	
	if args.collection_2 == None:
		args.collection_2 = 't2'
	if args.join_key_2 == None:
		args.join_key_2 = ['_id']
	if args.select_out_2 == None:
		args.select_out_2 = ['data']

	if args.result <> None:
		args.result = str(args.result)
	else:
		import datetime
		args.result = datetime.datetime.now().strftime("tmp_%Y-%m-%d_%H%M%S")

	from pymongo import MongoClient
	client = MongoClient(args.host)
	# SQL database
	database = client[args.database]

	# http://docs.mongodb.org/manual/reference/method/db.collection.mapReduce/
	from bson.code import Code
	from bson.son import SON

	mapper_key = ''
	if len(args.join_key_1) == 1:
		mapper_key = 'this.'+str(args.join_key_1[0])
	else:
		for field in args.join_key_1:
			if mapper_key == '':
				mapper_key = '{"'+str(field)+'": this.'+field
			else:
				mapper_key = mapper_key + ', "'+str(field)+'": this.'+field
		if mapper_key <> '':
			mapper_key = mapper_key + '}'

	mapper_value = ''
	for field in args.select_out_1:
		if mapper_value == '':
			mapper_value = '{"'+str(field)+'": this.'+field
		else:
			mapper_value = mapper_value + ', "'+str(field)+'": this.'+field

	if mapper_value <> '':
		mapper_value = mapper_value + '}'


	mapper_code = """
		function() {
			emit("""+mapper_key+""", """+mapper_value+""");
		}
	"""

	reducer_code = """
		function(key, values) {
			var out = {};
			values.forEach( function(value) {
				for ( field in value )
					if( value.hasOwnProperty(field) ) 
						out[field] = value[field];

			});
			return out
		}
	"""

	finalize_code = """
		function(key, res) {
			return res;
		}
	"""

	print "Mapper 1 Code: ", mapper_code
	print "Reducer 1 Code: ", reducer_code

	if args.reset_result:
		database[args.result].drop()

	results = None
	# http://api.mongodb.org/python/2.0/examples/map_reduce.html
	results = database[args.collection_1].map_reduce(
		Code ( mapper_code ), 
		Code( reducer_code ), 
		#args.result,
		out = SON( [ ("reduce", args.result)] ) ,
		#finalize = Code( finalize_code) ,
		full_response=True
	)
	print results

	if args.show_result:
		for doc in database[args.result].find():
			print doc

	mapper_key = ''
	if len(args.join_key_2) == 1:
		mapper_key = 'this.'+str(args.join_key_2[0])
	else:
		for field in args.join_key_2:
			if mapper_key == '':
				mapper_key = '{"'+str(field)+'": this.'+field
			else:
				mapper_key = mapper_key + ', "'+str(field)+'": this.'+field
		if mapper_key <> '':
			mapper_key = mapper_key + '}'

	mapper_value = ''
	for field in args.select_out_2:
		if mapper_value == '':
			mapper_value = '{"'+str(field)+'": this.'+field
		else:
			mapper_value = mapper_value + ', "'+str(field)+'": this.'+field

	if mapper_value <> '':
		mapper_value = mapper_value + '}'

	mapper_code = """
		function() {
			emit("""+mapper_key+""", """+mapper_value+""");
		}
	"""
	print "Mapper 2 Code: ", mapper_code
	print "Reducer 2 Code: ", reducer_code

	results = database[args.collection_2].map_reduce(
		Code ( mapper_code ), 
		Code( reducer_code ), 
		out = SON( [ ("reduce", args.result)] ) ,
		#args.result ,
		full_response=True
	)
	print results

	if args.show_result:
		for doc in database[args.result].find():
			print doc

	if args.delete_result:
		database[args.result].drop()

	client.close()
