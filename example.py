

from poolQueue import PoolQueue

from multiprocessing import Queue, Pool, cpu_count, Event
import queue
import time 
from threading import Thread

def jobExample(jobQueue: Queue, returnQueue: Queue, shouldStop: Event):

	# use this area for static variables needed for job 
	
	while not shouldStop.is_set():
		
		try:
			data = jobQueue.get(timeout = 0.5)
		except queue.Empty:
			time.sleep(0.001)
			continue

		time.sleep(0.1)
		
		returnQueue.put([data, data])

if __name__ == "__main__":

	easyPool = PoolQueue(jobExample)
	
	easyPool.start()
	
	for i in range(0, 100):
		easyPool.send(i)
		
	data = easyPool.join()
	
	print("done")
	
	