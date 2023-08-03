

from multiprocessing import Queue, Pool, cpu_count, Event
import queue
import time 
from threading import Thread
from typing import Callable
import sys
import os 

class PoolQueue:

	@staticmethod
	def returnQueueThreadWorker(returnQueue: Queue, shouldStop: Event, returnData: dict, progressQueue: queue.Queue):
		
		while not shouldStop.is_set():
			try:
				data, res = returnQueue.get(timeout = 0.1)
				returnData[data] = res
				progressQueue.put("job done")
			except queue.Empty:
				time.sleep(0.0001)
				continue
		
		pass
		
	@staticmethod
	def progressThreadWorker(progressQueue: queue.Queue, shouldStop: Event):
		
		jobsTotal = 0
		jobsDone = 0
		
		while not shouldStop.is_set():
			try:
				data = progressQueue.get(timeout = 0.1)
			except queue.Empty:
				time.sleep(0.0001)
				continue
				
			if data == "job done":
				jobsDone += 1
			elif data == "job added":
				jobsTotal += 1
			
			empty = "\u2591"
			full = "\u2588"
			
			width = os.get_terminal_size()[0]
			width = int(width * 0.5)
			
			progress = jobsDone / jobsTotal
			
			fullBar = int(width * jobsDone / jobsTotal)
			emptyBar = width - fullBar
			
			bar = "[" + full * fullBar + empty * emptyBar + "]"
			
			sys.stdout.write("Progress: {:s}  {:6.2f}% {:5d}/{:d}\r".format(bar, 100 * progress, jobsDone, jobsTotal))
			sys.stdout.flush()
		
		sys.stdout.write("\n")
		sys.stdout.flush()
		pass

	def __init__(self, targetFunction: Callable[[Queue, Queue, Event], None], cpuPercent = 0.5):
	
		cpuPercent = min(cpuPercent, 0.9)
	
		self.cpuCount = max(1, int(cpuPercent * cpu_count()))
		self.targetFunction = targetFunction
		
		self.jobQueue = Queue(maxsize = self.cpuCount * 5)
		self.returnQueue = Queue(maxsize = self.cpuCount * 10)
		
		# not a multiprocessed queue on purpose.
		self.progressQueue = queue.Queue()
		
		self.shouldStop = Event()
		self.shouldStop.clear()
		
		self.returnQueueShouldStop = Event()
		self.returnQueueShouldStop.clear()
		
		self.progressQueueShouldStop = Event()
		self.progressQueueShouldStop.clear()
		
		self.p = None
		
		self.isJoining = False
		self.isRunning = False
		
		self.returnData = None
		self.returnQueueThread = None

	def send(self, data):
		
		if not self.isRunning:
			print("start the pool before sending anything")
			return
		
		if self.isJoining:
			print("do not send data while joining!")
			return
		
		self.jobQueue.put(data)
		
		self.progressQueue.put("job added")

	def start(self):
		self.isRunning = True

		self.p = Pool(self.cpuCount, self.targetFunction, (self.jobQueue, self.returnQueue, self.shouldStop,))
	
		self.returnData = {}
		self.returnQueueThread = Thread(target=self.returnQueueThreadWorker, args=(self.returnQueue, self.returnQueueShouldStop, self.returnData, self.progressQueue))
		self.returnQueueThread.start()
		
		self.progressQueueThread = Thread(target=self.progressThreadWorker, args=(self.progressQueue, self.progressQueueShouldStop))
		self.progressQueueThread.start()
		
		print("pool started with " + str(self.cpuCount) + " cpus")
	
	def join(self):
	
		if not self.isRunning:
			print("pool must be running to join")
			return 
			
		self.isJoining = True
		
		while not self.jobQueue.empty():
			time.sleep(0.1)
			
		self.shouldStop.set()
		
		self.p.close()
		self.p.join()
		
		time.sleep(0.01)
		
		while not self.returnQueue.empty():
			time.sleep(0.1)
		
		self.returnQueueShouldStop.set()
		self.returnQueueThread.join()
		
		while not self.progressQueue.empty():
			time.sleep(0.01)
		
		self.progressQueueShouldStop.set()
		self.progressQueueThread.join()
		
		self.isRunning = False
		self.isJoining = False
		
		print("pool joined, returning data")
		
		return self.returnData


		