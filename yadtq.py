# yadtq.py
import json
import time
import logging
from kafka import KafkaProducer, KafkaConsumer
from backend import Backend
import redis
import threading

logger = logging.getLogger(__name__)

class YADTQ:
    def __init__(self, topic, backend=None):
        """
        Initialize YADTQ with topic and optional backend
        
        Args:
            topic (str): Kafka topic name
            backend (Backend, optional): Backend instance. If None, creates new one
        """
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        self.worker_failure_thread = threading.Thread(
            target=self.detect_and_handle_worker_failures, 
            daemon=True  # Allows thread to exit when main program exits
        )
        self.worker_failure_thread.start()
        
        # Initialize Redis and Backend
        if backend is None:
            self.redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
            self.backend = Backend(self.redis_client)
        else:
            self.backend = backend
            self.redis_client = backend.redis_client

    def assign_task(self, task_id, task_data):
        """Assign a task to the queue with deduplication"""
        pipe = self.redis_client.pipeline()
    
        # Store task data for potential requeuing
        pipe.hset("task_data", task_id, json.dumps(task_data))
        pipe.set("task_topic", self.topic)
    
        # Check if task already exists
        existing_status = self.backend.get_status(task_id)
        if existing_status and existing_status["status"] in ["processing", "success"]:
            return False
    
        # Mark task as queued in Redis
        self.backend.update_status(task_id, "queued")
    
        # Send to Kafka queue
        self.producer.send(self.topic, task_data)
        self.producer.flush()
    
        pipe.execute()
        return True

    def track_task_status(self, task_id):
        """Track the status of a task"""
        return self.backend.get_status(task_id)

    def detect_and_handle_worker_failures(self, timeout=5):
        """
        Monitor worker heartbeats and handle failures
        
        Args:
            timeout (int): Seconds since last heartbeat to consider a worker dead
        """
        while True:
            try:
                current_time = time.time()
            
                # Get all active workers
                active_workers = self.redis_client.smembers("active_workers")
                
                for worker_id_bytes in active_workers:
                    worker_id = worker_id_bytes.decode()
                    
                    # Get worker status and last heartbeat
                    worker_status = self.backend.get_worker_status(worker_id)
                    
                    # Check if worker has missed heartbeats
                    if (worker_status['status'] == 'alive' and 
                        worker_status['last_heartbeat'] is not None and 
                        current_time - worker_status['last_heartbeat'] > timeout):
                        
                        logger.warning(f"Worker {worker_id} missed heartbeat. Marking as dead.")
                        
                        # Mark worker as dead and handle its tasks
                        self.backend.mark_worker_dead(worker_id)
            
            except Exception as e:
                logger.error(f"Error in worker failure detection: {str(e)}")
            
            # Sleep to avoid constant polling
            time.sleep(timeout/2)
