#!/usr/bin/env python

# M-Harris
# mySDB : mySlowDataBase
# SDBCluster
# 6-08-2018

import threading
import logging
import socket

logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-4s) %(message)s',)

class SDBCluster():
	def __init__(self, myIP, myPORT):
		self.lock = threading.Lock()

		# List of tuples for other mySDB servers
		# (IP, PORT)
		self.mySDB_cluster_dict = {}
		self.myIP = myIP
		self.myPORT = myPORT
		logging.debug("Current server in cluster: " + str((self.myIP, self.myPORT)))

	def addServer(self, ip, port):
		self.lock.acquire()
		try:
			if not (ip, port) in self.mySDB_cluster_dict:
				s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				try:
					logging.debug("Trying to connect to server: " + str((ip, port)))
					s.connect((ip, port))
					s.send("CLUSTER MEET " + str(self.myIP) + " " + str(self.myPORT))
					self.mySDB_cluster_dict[(ip, port)] = s
					logging.debug("Added server to cluster: " + str((ip, port)))
					return True
				except:
					logging.debug("Can't find server: " + str((ip, port)))
					return False
			return True
		finally:
			self.lock.release()

	def removeDeadServers(self):
		self.lock.acquire()
		try:
			delete_lst = []
			logging.debug("Removing dead servers.")

			for k, s in self.mySDB_cluster_dict.items():
				try:
					ready_to_read, ready_to_write, in_error = select.select([s,], [s,], [], 1)
				except:
					logging.debug("Can't find server: " + str(s.getsockname()))
					delete_lst.append(k)

			for k in delete_lst:
				logging.debug("Removing server from client sockets list: " + str(k))
				s = self.mySDB_cluster_dict[k]
				s.close()
				logging.debug("Closed connection to server")
				del self.mySDB_cluster_dict[k]
				logging.debug("Removed server from client sockets list: " + str(k))

			logging.debug("Done removing dead servers.")
		finally:
			self.lock.release()

	def writeToCluster(self, key, value):
		self.lock.acquire()
		try:
			logging.debug("Writing to cluster: " + str((key, value)))
			delete_lst = []

			for k, s in self.mySDB_cluster_dict.items():
				try:
					s.send("SET " + str(key) + " " + str(value))
				except:
					logging.debug("Can't find server: " + str(s.getsockname()))
					delete_lst.append(k)

			for k in delete_lst:
				logging.debug("Removing server from client sockets list: " + str(k))
				s = self.mySDB_cluster_dict[k]
				s.close()
				logging.debug("Closed connection to server")
				del self.mySDB_cluster_dict[k]
				logging.debug("Removed server from client sockets list: " + str(k))

		finally:
			self.lock.release()

	def deleteFromCluster(self, key):
		self.lock.acquire()
		try:
			logging.debug("Deleting from cluster: " + str(key))
			delete_lst = []

			for k, s in self.mySDB_cluster_dict.items():
				try:
					s.send("DEL " + str(key))
				except:
					logging.debug("Can't find server: " + str(s.getsockname()))
					delete_lst.append(k)

			for k in delete_lst:
				logging.debug("Removing server from client sockets list: " + str(k))
				s = self.mySDB_cluster_dict[k]
				s.close()
				logging.debug("Closed connection to server")
				del self.mySDB_cluster_dict[k]
				logging.debug("Removed server from client sockets list: " + str(k))

		finally:
			self.lock.release()

	def close(self):
		self.lock.acquire()
		try:
			logging.debug("Deleting self from cluster")
			for k, s in self.mySDB_cluster_dict.items():
				try:
					logging.debug("Closing connection to server: " + str(s.getsockname()))
					s.close()
					logging.debug("Closed connection to server")
				except:
					logging.debug("Connection already closed.")

		finally:
			self.lock.release()