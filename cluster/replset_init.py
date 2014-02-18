# -*- coding: utf-8 -*-

import argparse, os, glob, sys, subprocess, time, shutil, json
from pymongo import MongoClient

parser = argparse.ArgumentParser(description='Mongodb Cluster')
parser.add_argument('--db-dir', metavar='DB Path', type=str, help='path for databases')
parser.add_argument('--log-dir', metavar='DB Log Path', type=str, help='path for databases log')
parser.add_argument('--node-num', metavar='Node number', type=int, default=3)
parser.add_argument('--port-base', metavar='service port number', type=int, default=30000)
parser.add_argument('--stop', action='store_true', default=False)
parser.add_argument('--reset', action='store_true', default=False)
parser.add_argument('--repl-set', metavar='replSet name', type=str, default="firstset")
parser.add_argument('--auth-key-file', metavar='DB Auth', type=str, default=None)
parser.add_argument('--auth-user', metavar='DB Login account for init', type=str, default=None)
parser.add_argument('--auth-pass', metavar='DB Login password for init', type=str, default=None)

def mongod_init(i, mode=0, info_dir={'db_dir':None, 'log_dir':None, 'pid_dir':None}, replica_set_name="firtset", key_file_path=None, replica_set_command={}, port_base=30000):
	target_db_path = info_dir['db_dir'] + "/db-" + str(i)
	target_log_path = info_dir['log_dir'] + "/db-" + str(i) + ".log"
	target_pid_path = info_dir['log_dir'] + "/db-" + str(i) + ".pid"
	target_cmd_path = info_dir['log_dir'] + "/db-" + str(i) + ".cmd"
	target_db_port = port_base + i
	print "Init DB("+str(i)+"), Port: "+str(target_db_port)+", Path: "+target_db_path
	if not os.path.exists(target_db_path):
		os.makedirs(target_db_path)

	replica_set_command['replSetInitiate']['members'].append( {
		'_id': i+1, 
		'host': 'localhost:'+str(target_db_port),
	} )		

	cmds = [ 
		"nohup" ,
		"mongod" , 
			"--dbpath", target_db_path, 
			"--port", str(target_db_port), 
			#"--replSet", replica_set_name,
			"--oplogSize", str(700), 
			"--logpath", target_log_path,
			"--rest",
			#"--setParameter", "enableLocalhostAuthBypass=1"
	] 

	if mode == 0:	# no auth
		cmds.append("--replSet")
		cmds.append(replica_set_name)
		pass
	elif mode == 1:	# setup auth without replSet
		pass
	elif mode == 2: # setup auth with keyfile
		cmds.append("--replSet")
		cmds.append(replica_set_name)
		cmds.append("--keyFile")
		cmds.append(key_file_path)

	print " $", " ".join(cmds[1:])
	print
	#subprocess.call([ 
	process = subprocess.Popen(cmds)

	# write pid
	pid_file = open(target_pid_path, 'w')
	pid_file.write(str(process.pid))
	pid_file.close()

	# write cmd
	cmd_file = open(target_cmd_path, 'w')
	cmd_file.write(str(" ".join(cmds[1:])))
	cmd_file.close()

	return process

if __name__ == "__main__":
	args = parser.parse_args()
	if args.db_dir == None:
		args.db_dir = os.path.dirname(os.path.realpath(__file__))+'/db'
	if args.log_dir == None:
		args.log_dir = os.path.dirname(os.path.realpath(__file__))+'/log'

	# kill all process
	for pid_file in glob.glob(args.log_dir+'/'+'*.pid'):
		pid = open(pid_file, 'r').read().strip()
		if pid != '':
			print "kill: "+ pid
			subprocess.call([ 'kill', pid ] )
		os.remove(pid_file)

	if args.reset:
		for target in [args.db_dir, args.log_dir]:
			if os.path.exists(target):
				try:
					shutil.rmtree(target)
				except Exception, e:
					print e
	if args.stop:
		sys.exit(0)

	for target in [args.db_dir, args.log_dir]:
		if not os.path.exists(target):
			os.makedirs(target)

	# mongo_command
	replica_set_command = {
		"replSetInitiate": {
			"_id": args.repl_set ,
			"members": []
		}
	}

	# http://cookbook.mongodb.org/operations/convert-replica-set-to-replicated-shard-cluster/
	base_server = None

	if args.auth_key_file == None:
		# Step 1: run mongod
		for i in range(args.node_num):
			mongod_init(i, 
				mode=0, 
				info_dir={'db_dir': args.db_dir, 'log_dir': args.log_dir, 'pid_dir': args.log_dir },
				replica_set_name=args.repl_set,
				replica_set_command=replica_set_command,
				port_base=args.port_base
			)

		# Step 2: wait for mongod ready
		print "Waiting..."
		for info in replica_set_command['replSetInitiate']['members']:
			check = True
			sys.stdout.write("\t"+info['host']+": ")
			while check:
				if base_server == None:
					base_server = info['host']
				try:
					client = MongoClient(info['host'])
					client.close()
					check = False
				except Exception, e:
					#print e
					pass
				if check:
					sys.stdout.write('.')
					sys.stdout.flush()
					time.sleep(1)
				else:
					sys.stdout.write('OK\n')


		print
		print "Connect to", base_server
		print
		init_log_file = args.log_dir + '/reli-set-init.log'
		if os.path.exists(init_log_file):
			print "init done"
		else:
			# Initialize the First Replica Set
			# db.runCommand()

			client = MongoClient(base_server)
			print "Initialize the First Replica Set: "
			print
			print "$ monogo "+str(base_server)+"/admin"
			print "mongo> db.runCommand(", replica_set_command, ")"
			print
			print "Result:"
			try:
				ret = client.admin.command(replica_set_command)
				print ret
				if ret['ok'] == 1.0:
					fd = open(init_log_file,'w')
					json.dump(replica_set_command, fd)
					fd.close()
			except Exception, e:
				print e
			client.close()

	else:
		# Step 0: auth usage init
		if args.auth_user <> None and args.auth_pass <> None:
			tmp = {'replSetInitiate':{'members':[]}}
			process = mongod_init(0, 
				mode=1, 
				info_dir={'db_dir': args.db_dir, 'log_dir': args.log_dir, 'pid_dir': args.log_dir },
				replica_set_command=tmp,
				replica_set_name=args.repl_set,
				port_base=args.port_base
			)

			print "Waiting..."
			for info in tmp['replSetInitiate']['members']:
				check = True
				sys.stdout.write("\t"+info['host']+": ")
				while check:
					if base_server == None:
						base_server = info['host']
					try:
						client = MongoClient(info['host'])
						client.close()
						check = False
					except Exception, e:
						#print e
						pass
					if check:
						sys.stdout.write('.')
						sys.stdout.flush()
						time.sleep(1)
					else:
						sys.stdout.write('OK\n')
	
			# add account
			print
			print "Connect to", base_server
			print

			client = MongoClient(base_server)
			print "Initialize the First Replica Set: "
			print
			print "$ monogo "+str(base_server)+"/admin"
			print """mongo> db.addUser( {user: "account", pwd:"password", roles:["userAdminAnyDatabase"] })"""
			print
			add_account_ret = None
			try:
				#ret = client.admin.command('db.addUser( {user: "'+args.auth_user+'", pwd:"'+args.auth_pass+'", roles:["userAdminAnyDatabase"] })')
				add_account_ret = client.admin.add_user(args.auth_user, args.auth_pass)
			except Exception, e:
				print e
			client.close()

			if add_account_ret <> None:
				print "Error:", add_account_ret
				sys.exit(1)
			print "Add account done."

			print
			print "Restart the mongod:"
			print
			process.kill()

		# Step 1: start mongod
		for i in range(args.node_num):
			mongod_init(i, 
				mode=2, 
				info_dir={'db_dir': args.db_dir, 'log_dir': args.log_dir, 'pid_dir': args.log_dir },
				replica_set_name=args.repl_set,
				replica_set_command=replica_set_command,
				key_file_path=args.auth_key_file,
				port_base=args.port_base
			)

		# Step 2: wait for mongod ready
		print "Waiting..."
		for info in replica_set_command['replSetInitiate']['members']:
			check = True
			sys.stdout.write("\t"+info['host']+": ")
			while check:
				if base_server == None:
					base_server = info['host']
				try:
					client = MongoClient(info['host'])
					client.close()
					check = False
				except Exception, e:
					#print e
					pass
				if check:
					sys.stdout.write('.')
					sys.stdout.flush()
					time.sleep(1)
				else:
					sys.stdout.write('OK\n')


		print
		print "Connect to", base_server
		print
		init_log_file = args.log_dir + '/reli-set-init.log'
		if os.path.exists(init_log_file):
			print "init done"
		else:
			# Initialize the First Replica Set
			# db.runCommand()

			client = MongoClient(base_server)
			print "Initialize the First Replica Set: "
			print
			print "$ monogo "+str(base_server)+"/admin"
			print "mongo> db.runCommand(", replica_set_command, ")"
			print
			print "Result:"
			try:
				if client.admin.authenticate(args.auth_user, args.auth_pass):
					ret = client.admin.command(replica_set_command)
					print ret
					if ret['ok'] == 1.0:
						fd = open(init_log_file,'w')
						json.dump(replica_set_command, fd)
						fd.close()
				else:
					print "auth login error"
			except Exception, e:
				print e
			client.close()
		
	print
	print "All is done."
	print 
	print "server info:"
	for info in replica_set_command['replSetInitiate']['members']:
		if args.auth_key_file:
			print " $ mongo "+info['host']+"/admin -u account -p password --eval 'printjson(rs.status())'"
		else:
			print " $ mongo "+info['host']+"/admin --eval 'printjson(rs.status())'"
		break
	print
	print "shutdown servers:"
	for info in replica_set_command['replSetInitiate']['members']:
		if args.auth_key_file:
			print " $ mongo "+info['host']+"/admin -u account -p password --eval 'db.shutdownServer()'"
		else:
			print " $ mongo "+info['host']+"/admin --eval 'db.shutdownServer()'"
