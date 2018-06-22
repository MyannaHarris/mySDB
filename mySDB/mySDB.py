#!/usr/bin/env python

# M-Harris
# mySDB : mySlowDataBase
# 5-16-2018
#
# Running on localhost:
# Server
# python mySDB.py 
# or
# python mySDB.py <PORT_NUMBER>
# Ex: python mySDB.py 6377
#
# Client
# telnet <HOST> <PORT_NUMBER>
# Ex: telnet 127.0.0.1 6379
#
# Commnads
# SET <key> <value>
# Ex: SET test "This is a test."
#
# GET <key>
# Ex; GET test
#
# DEL <key1> <key2> ...
# Ex: DEL test
# Ex: DEL test1 test2
#
# CLUSTER MEET <HOST> <PORT_NUMBER>
# Ex: CLUSTER MEET 127.0.0.1 6377

import sys
import socket
import threading
from SDBDictionary import SDBDictionary
from Connection import Connection
from SDBCluster import SDBCluster
import logging
import select

logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-4s) %(message)s',)

HOST = '127.0.0.1' # Loopback to localhost
PORT = 6379 # Port like Redis
BUFFER_SIZE = 1024 # Buffer size is normally 1024

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

mySDB_dict = SDBDictionary()

def main():

	args = sys.argv[1:]
	if len(args) > 0:
		port_num = int(args[0])
	else:
		port_num = PORT

	print("Port: " + str(port_num))

	s.bind((HOST,port_num))
	s.listen(0)

	mySDB_cluster = SDBCluster(HOST,port_num)

	connections = []

	try:
		while 1:
			readConn, _, _ = select.select([s], [], [], 1)
			if bool(readConn):
				conn, addr = s.accept()
				connection = Connection(conn, addr, mySDB_dict, BUFFER_SIZE, mySDB_cluster)
				connections.append(connection)
				print("Number of connections: " + str(len(connections)))
				logging.debug('Starting %s', connection.getName())
				connection.start()

			len_conns = len(connections)
			for i in range(0,len_conns):
				if i >= len(connections):
					break
				curr_conn = connections[i]
				if curr_conn.isClosed():
					logging.debug('Joining %s', curr_conn.getName())
					curr_conn.join()
					logging.debug('Finished joining %s', curr_conn.getName())
					del connections[i]
					print("Number of connections: " + str(len(connections)))
					break


	except KeyboardInterrupt:
		print("Closing all connections to port.")
		for c in connections:
			c.join()
		logging.debug('Finished joining all connections')
		mySDB_cluster.close()
		s.close()
		logging.debug('Closed socket connection')
		sys.exit(0)

if __name__ == '__main__':
	main()
    
