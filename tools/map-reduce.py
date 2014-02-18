# -*- coding: utf-8 -*-
import argparse
parser = argparse.ArgumentParser(description='Mongodb log')
parser.add_argument('--host', metavar='DB Host', type=str, help='the ip for database host')
parser.add_argument('--database', metavar='Collections(SQL:Database)', type=str, help='a name for database(database name for SQL DB)')
parser.add_argument('--collection', metavar='Collection(SQL:Table)', type=str, help='a name for collection (table name for SQL DB)')
parser.add_argument('--key', metavar='Key(SQL:Field)', type=str, help='a key for collection (table field for SQL DB)')
parser.add_argument('--value-array-type', action="store_true", help='set if value is array type')
parser.add_argument('--result', metavar='Collection(SQL:Table)', help='a collection for result')
parser.add_argument('--show-result', action="store_true", help='print the result')
parser.add_argument('--reset-result', action="store_true", help='reset result before job')
parser.add_argument('--delete-result', action="store_true", help='delete result after job done')

if __name__ == "__main__":
	args = parser.parse_args()
	if args.database == None:
		args.database = 'db'
	if args.collection == None:
		args.collection = 'test'
	if args.host <> None:
		args.host = str(args.host)
	if args.key <> None:
		args.key = str(args.key)
	else:
		args.key = 'field'
	if args.result <> None:
		args.result = str(args.result)
	else:
		import datetime
		args.result = datetime.datetime.now().strftime("tmp_%Y-%m-%d_%H%M%S")

	from pymongo import MongoClient
	client = MongoClient(args.host)
	# SQL database
	database = client[args.database]
	# SQL table
	collection = database[args.collection]

	# http://docs.mongodb.org/manual/reference/method/db.collection.mapReduce/
	from bson.code import Code

	mapper = None
	if  args.value_array_type:
		mapper = Code( 
			"""
			function() {
				this."""+str(args.key)+""".forEach(function(z) {
					emit(z, 1);
				});
			}
			"""
		) 
	else:
		mapper = Code ( 
			"""
			function() {
				emit(this."""+str(args.key)+""", 1);
			}
			"""
		)

	reducer = Code(
		"""
		function(key, value) {
			var total = 0;
			for(var i = 0 ; i < value.length ; ++i ) {
				total += value[i];
			}
			return total;
		}
		"""
	)
	
	if args.reset_result:
		database[args.result].drop()

	results = collection.map_reduce(mapper, reducer, args.result)
	print results

	if args.show_result:
		for doc in database[args.result].find():
			print doc

	if args.delete_result:
		database[args.result].drop()

	client.close()
