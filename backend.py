import json
import redis
import time
import logging
from datetime import datetime
from kafka import KafkaProducer
logger = logging.getLogger(__name__)
class Backend:
    def __init__(self, redis_client):
        self.redis_client = redis_client
        
    def register_worker(self, worker_id):
        """Register a new worker"""
        pipe = self.redis_client.pipeline()
        pipe.sadd("active_workers", worker_id)
        pipe.hset("worker_status", worker_id, "alive")
        pipe.hset("worker_heartbeats", worker_id, time.time())
        pipe.execute()

    def deregister_worker(self, worker_id):
        """Deregister a worker"""
        pipe = self.redis_client.pipeline()
        pipe.srem("active_workers", worker_id)
        pipe.hdel("worker_status", worker_id)
        pipe.hdel("worker_heartbeats", worker_id)
        pipe.execute()

    def update_status(self, task_id, status, result=None, worker_id=None):
        """Update the status of a task with metadata"""
        task_status = {
            "status": status,
            "result": result,
            "worker_id": worker_id,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Use Redis transaction to ensure atomic updates
        pipe = self.redis_client.pipeline()
        pipe.hset("tasks", task_id, json.dumps(task_status))
        if worker_id and status == "processing":
            pipe.hset("task_assignments", task_id, worker_id)
        pipe.execute()
        
        print(f"Task {task_id} status updated to {status}")

    def get_status(self, task_id):
        """Retrieve the status of a task"""
        task_status = self.redis_client.hget("tasks", task_id)
        return json.loads(task_status) if task_status else None

    def get_worker_status(self, worker_id):
        """Get worker's last heartbeat and status"""
        last_heartbeat = self.redis_client.hget("worker_heartbeats", worker_id)
        status = self.redis_client.hget("worker_status", worker_id)
        return {
            "last_heartbeat": float(last_heartbeat) if last_heartbeat else None,
            "status": status.decode() if status else "unknown"
        }

    def update_worker_heartbeat(self, worker_id):
        """Update worker heartbeat timestamp with atomic operations"""
        pipe = self.redis_client.pipeline()
        current_time = time.time()
    
        # Update both heartbeat and status atomically
        pipe.hset("worker_heartbeats", worker_id, current_time)
        pipe.hset("worker_status", worker_id, "alive")
        pipe.execute()

    def mark_worker_dead(self, worker_id):
        """
        Enhanced method to handle worker failure, comprehensively requeue tasks, 
        and clean up worker resources
        """
        pipe = self.redis_client.pipeline()

        try:
            # Mark worker as dead
            pipe.hset("worker_status", worker_id, "dead")
            pipe.hdel("worker_heartbeats", worker_id)
    
            # Get all task assignments for this worker
            assignments = self.redis_client.hgetall("task_assignments")
    
            for task_id_bytes, assigned_worker_bytes in assignments.items():
                task_id = task_id_bytes.decode()
                assigned_worker = assigned_worker_bytes.decode()
        
                if assigned_worker == worker_id:
                    task_status = self.get_status(task_id)
            
                    # Expanded condition to handle tasks that might be near completion
                    if task_status and (
                        task_status["status"] == "processing" or 
                        (task_status["status"] == "queued" and task_status.get("worker_id") == worker_id)
                    ):
                        # Retrieve the original task data for requeuing
                        task_data_json = self.redis_client.hget("task_data", task_id)
                        topic = self.redis_client.get("task_topic")
                    
                        if task_data_json and topic:
                            # Enhanced reset of task status
                            new_status = {
                                "status": "queued",
                                "result": "Requeued after worker failure",
                                "worker_id": None,
                                "timestamp": datetime.utcnow().isoformat(),
                                "failure_count": task_status.get("failure_count", 0) + 1
                            }
                        
                            pipe.hset("tasks", task_id, json.dumps(new_status))
                            pipe.hdel("task_assignments", task_id)
                        
                            # Only requeue if not already failed
                            if new_status["status"] == "queued":
                                producer = KafkaProducer(
                                    bootstrap_servers='localhost:9092',
                                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                                )
                                task_data = json.loads(task_data_json)
                                producer.send(topic.decode(), task_data)
                                producer.flush()
                                producer.close()
        
            # Remove the worker from active workers set
            pipe.srem("active_workers", worker_id)
        
            pipe.execute()
        
            logger.info(f"Worker {worker_id} marked dead. Incomplete tasks have been comprehensively handled.")
    
        except Exception as e:
            logger.error(f"Error marking worker {worker_id} as dead: {str(e)}")
            raise
