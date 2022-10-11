

from multiprocessing import Queue, Pool, cpu_count, Event
import queue
import time 
from threading import Thread

class PoolQueue:

	@staticmethod
	def returnQueueThreadWorker(returnQueue, shouldStop, returnData):
		
		while not shouldStop.is_set():
			try:
				data, res = returnQueue.get(timeout = 0.1)
				returnData[data] = res
			except queue.Empty:
				time.sleep(0.0001)
				continue
		
		pass

	def __init__(self, targetFunction, cpuPercent = 0.5):
	
		cpuPercent = min(cpuPercent, 0.9)
	
		self.cpuCount = max(1, int(cpuPercent * cpu_count()))
		self.targetFunction = targetFunction
		
		self.jobQueue = Queue(maxsize = self.cpuCount * 5)
		self.returnQueue = Queue(maxsize = self.cpuCount * 10)
		
		self.shouldStop = Event()
		self.shouldStop.clear()
		
		self.returnQueueShouldStop = Event()
		self.returnQueueShouldStop.clear()
		
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

	def start(self):
		self.isRunning = True

		self.p = Pool(self.cpuCount, self.targetFunction, (self.jobQueue, self.returnQueue, self.shouldStop,))
	
		self.returnData = {}
		self.returnQueueThread = Thread(target=self.returnQueueThreadWorker, args=(self.returnQueue, self.returnQueueShouldStop, self.returnData))
		self.returnQueueThread.start()
		
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
		
		self.isRunning = False
		self.isJoining = False
		
		print("pool joined, returning data")
		
		return self.returnData
	
