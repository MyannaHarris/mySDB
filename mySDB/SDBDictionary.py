#!/usr/bin/env python

# M-Harris
# mySDB : mySlowDataBase
# SDBDictionary
# 6-01-2018

import threading
import logging

logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-4s) %(message)s',)

class SDBDictionary():
	def __init__(self):
		self.lock = threading.Lock()
		self.mySDB_dict = {}

	def set(self, key, value):
		self.lock.acquire()
		try:
			logging.debug('Setting: ' + str(key) + " -> " + str(value))
			self.mySDB_dict[key] = value
			logging.debug('Set: ' + str(key) + " -> " + str(value))
		finally:
			self.lock.release()

	def get(self, key):
		self.lock.acquire()
		value = ""
		try:
			logging.debug('Getting: ' + str(key))
			value = self.mySDB_dict.get(key, "")
			logging.debug('Got value: ' + str(value))
		finally:
			self.lock.release()
		return value


	def delete(self, key):
		self.lock.acquire()
		try:
			logging.debug('Deleting: ' + str(key))
			del self.mySDB_dict[key]
			logging.debug('Deleted: ' + str(key))
			return True
		except KeyError:
			logging.debug('Failed to delete: ' + str(key))
			return False
		finally:
			self.lock.release()

	def conatains(self, key):
		self.lock.acquire()
		try:
			return key in self.mySDB_dict
		finally:
			logging.debug('Checked if dictionary contains: ' + str(key))
			self.lock.release()