#!/usr/bin/env python

# M-Harris
# mySDB : mySlowDataBase
# Connection
# 6-01-2018

import threading
import logging
import select

logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-4s) %(message)s',)

class Connection(threading.Thread):
	def __init__(self, conn, addr, mySDB_dict, buffer_size, mySDB_cluster):
		threading.Thread.__init__(self)
		self.conn = conn
		self.addr = addr
		self.mySDB_dict = mySDB_dict
		self.isClosedBool = False
		self.buffer_size = buffer_size
		self.runThread = True
		self.mySDB_cluster = mySDB_cluster
		logging.debug("Running connection from: " + str(self.addr))

	def isClosed(self):
		return self.isClosedBool

	def run(self):
		self.conn.send("Use ^] or an empty string to close connection\n")
		while self.runThread:
			line = ""
			self.conn.send("mySDB>")
			while not line and self.runThread:
				try:
					readConn, _, _ = select.select([self.conn], [], [])
					if bool(readConn):
						line = self.conn.recv(self.buffer_size)
						line = line.strip()
						print "received data:", line
						logging.debug("Recieved data: " + str(line))
						if line == "":
							line = "^]"
				except:
					break

			if line == "^]":
				logging.debug("Closing connection from: " + str(self.addr))
				self.conn.close()
				self.mySDB_cluster.removeDeadServers()
				logging.debug("Connection closed from: " + str(self.addr))
				self.isClosedBool = True
				break

			line_lst = line.split()

			if len(line_lst) > 0:
				if line_lst[0] == "SET":
					if len(line_lst) >= 3:
						value = " ".join(line_lst[2:])
						if (not self.mySDB_dict.conatains(line_lst[1])) or (not self.mySDB_dict.get(line_lst[1]) == value):
							self.mySDB_dict.set(line_lst[1],value)
							self.conn.send("+OK\r\n")
							self.mySDB_cluster.writeToCluster(line_lst[1],value)
					else:
						self.conn.send("-SET requires at least 2 arguments deliminated by a space. Ex: SET <key> <value> (Where vaue can be multiple words.)\r\n")

				elif line_lst[0] == "GET":
					if len(line_lst) == 2:
						value = self.mySDB_dict.get(line_lst[1])
						if value:
							self.conn.send("$" + str(len(str(value))) + "\r\n" + str(value) + "\r\n")
						else:
							self.conn.send("-Unknown key: " + line_lst[1] + "\r\n")
					else:
						self.conn.send("-GET requires 1 argument. Ex: GET <key>\r\n")

				elif line_lst[0] == "DEL":
					if len(line_lst) < 2:
						self.conn.send("-DEL requires at least 1 argument, but can take multiple deliminated by a space. Ex: DEL <key1> <key2>\r\n")
					else:
						numDeleted = 0
						for i in range(1, len(line_lst)):
							delSuccessful = self.mySDB_dict.delete(line_lst[i])
							if delSuccessful:
								numDeleted += 1
								self.mySDB_cluster.deleteFromCluster(line_lst[i])
							else:
								self.conn.send("-Unknown key: " + str(line_lst[i]) +"\r\n")

						self.conn.send("$" + str(len(str(numDeleted))) + "\r\n" + str(numDeleted) + "\r\n")

				elif line_lst[0] == "CLUSTER" and len(line_lst) > 1 and line_lst[1] == "MEET":
					if len(line_lst) == 4:
						succeeded = self.mySDB_cluster.addServer(str(line_lst[2]), int(line_lst[3]))
						if succeeded:
							self.conn.send("+OK\r\n")
						else:
							self.conn.send("-Unknown server: " + str((line_lst[2], line_lst[3])) +"\r\n")
					else:
						self.conn.send("-CLUSTER MEET requires 2 arguments deliminated by a space. Ex: CLUSTER MEET <IP> <PORT> -> CLUSTER MEET 127.0.0.1 63202\r\n")

				else:
					self.conn.send("-Unknown command. Please use SET, GET, or DEL. Enter '^]' to exit.\r\n")
		return

	def join(self, timeout=None):
		logging.debug("Closing connection and joining from: " + str(self.addr))
		self.runThread = False
		self.conn.close()
		logging.debug("Joining now that connection is closed from: " + str(self.addr))
		super(Connection, self).join(timeout)