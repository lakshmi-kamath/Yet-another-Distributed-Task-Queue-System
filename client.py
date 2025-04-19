import sys
import uuid
import time
import json
import logging
from yadtq import YADTQ
from kafka import KafkaProducer
from kafka.errors import KafkaError
import redis
from tabulate import tabulate

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('Client')

def check_services():
    """Check if required services are running"""
    services_ok = True
    
    # Check Redis
    try:
        redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
        redis_client.ping()
        print("✓ Redis is running")
    except redis.ConnectionError:
        print("✗ Redis is not running")
        services_ok = False

    # Check Kafka
    try:
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        producer.close()
        print("✓ Kafka is running")
    except Exception:
        print("✗ Kafka is not running")
        services_ok = False

    return services_ok

class Client:
    def __init__(self, topic):
        self.topic = topic
        try:
            # Test Kafka connection
            producer = KafkaProducer(
                bootstrap_servers='localhost:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            producer.close()
            
            # Test Redis connection
            redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
            redis_client.ping()
            
            # Initialize YADTQ with Redis client
            from backend import Backend
            self.yadtq = YADTQ(topic=topic, backend=Backend(redis_client))
            # Store Redis client for getting task data
            self.redis_client = redis_client
            logger.info(f"Client initialized successfully for topic {topic}")
            
        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka: {str(e)}")
            raise
        except redis.ConnectionError as e:
            logger.error(f"Failed to connect to Redis: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Initialization error: {str(e)}")
            raise

    def get_task_details(self, task_id):
        """Get both task data and status"""
        task_data_json = self.redis_client.hget("task_data", task_id)
        task_data = json.loads(task_data_json) if task_data_json else None
        status = self.yadtq.track_task_status(task_id)
        return task_data, status

    def submit_tasks(self):
        """Submit a batch of tasks and return their IDs"""
        task_ids = []
        tasks = [
            {"task": "add", "args": [1, 2]},
            {"task": "subtract", "args": [5, 3]},
            {"task": "multiply", "args": [2, 3]},
            {"task": "add", "args": [10, 20]},
            {"task": "subtract", "args": [100.0, 50]},
            {"task": "add", "args": [11, 23]},
            {"task": "subtract", "args": [57, 3]},
            {"task": "multiply", "args": [21, 3]},
            {"task": "add", "args": [10, 200]},
            {"task": "subtract", "args": [105, 55]},
            {"task": "add", "args": [101, 200]},
            {"task": "divide", "args": [1051, 55]}
        ]

        logger.info(f"Preparing to submit {len(tasks)} tasks")

        for task_data in tasks:
            try:
                task_id = str(uuid.uuid4())
                task_data["task_id"] = task_id
                
                # Submit task
                self.yadtq.assign_task(task_id, task_data)
                task_ids.append(task_id)
                
                logger.info(f"Successfully submitted task {task_id}: {task_data}")
                
            except Exception as e:
                logger.error(f"Failed to submit task: {str(e)}")
                continue

        logger.info(f"Successfully submitted {len(task_ids)} tasks")
        return task_ids

    def format_task_operation(self, task_data):
        """Format task operation as a readable string"""
        if not task_data:
            return "Unknown"
        
        task = task_data.get("task", "")
        args = task_data.get("args", [])
        
        if task == "add":
            return f"{args[0]} + {args[1]}"
        elif task == "subtract":
            return f"{args[0]} - {args[1]}"
        elif task == "multiply":
            return f"{args[0]} × {args[1]}"
        elif task == "divide":
            return f"{args[0]} ÷ {args[1]}"
        return f"{task}({args})"

    def monitor_tasks(self, task_ids, timeout=120, check_interval=2):
        """Monitor tasks with enhanced display showing task details"""
        logger.info(f"Starting to monitor {len(task_ids)} tasks")
        start_time = time.time()
        completed_tasks = set()
        task_status_table = []
        headers = ["Task ID", "Operation", "Status", "Worker", "Result"]

        while len(completed_tasks) < len(task_ids):
            if time.time() - start_time > timeout:
                logger.warning("Task monitoring timed out!")
                break

            # Update task status table
            task_status_table = []
            for task_id in task_ids:
                try:
                    task_data, status = self.get_task_details(task_id)
                    
                    if not status:
                        status = {"status": "unknown", "worker_id": None, "result": None}
                    
                    # Create table row with enhanced information
                    row = [
                        task_id[:8],  # Shortened task ID
                        self.format_task_operation(task_data),  # Human readable operation
                        status.get("status", "unknown"),
                        status.get("worker_id", "none")[:8] if status.get("worker_id") else "none",
                        str(status.get("result", "")) if status.get("status") in ["success", "failed"] else "-"
                    ]
                    
                    task_status_table.append(row)
                    
                    # Check if task is complete
                    if status and status.get("status") in ["success", "failed"]:
                        completed_tasks.add(task_id)
                        
                except Exception as e:
                    logger.error(f"Error monitoring task {task_id}: {str(e)}")
                    task_status_table.append([task_id[:8], "Error", str(e), "-", "-"])

            # Clear screen and display updated table
            print("\033[H\033[J", end="")  # Clear screen
            print("\nTask Execution Status:\n")
            
            # Sort table by status (completed tasks at bottom)
            task_status_table.sort(key=lambda x: x[2] in ["success", "failed"])
            
            print(tabulate(task_status_table, headers=headers, tablefmt="grid"))
            
            # Print progress
            total = len(task_ids)
            completed = len(completed_tasks)
            print(f"\nProgress: {completed}/{total} tasks completed")
            
            # Print time elapsed
            elapsed = int(time.time() - start_time)
            print(f"Time elapsed: {elapsed} seconds")

            if len(completed_tasks) < len(task_ids):
                time.sleep(check_interval)

        # Final summary
        print("\nExecution Summary:")
        print("-" * 50)
        successful = sum(1 for row in task_status_table if row[2] == "success")
        failed = sum(1 for row in task_status_table if row[2] == "failed")
        print(f"Total tasks: {len(task_ids)}")
        print(f"Successful: {successful}")
        print(f"Failed: {failed}")
        print(f"Incomplete: {len(task_ids) - len(completed_tasks)}")
        print(f"Total time: {int(time.time() - start_time)} seconds")

        return {task_id: status for task_id, status in 
               [(tid, self.get_task_status(tid)) for tid in task_ids]}
               
    def get_task_status(self, task_id):
        """Get the status of a specific task"""
        try:
            status = self.yadtq.track_task_status(task_id)
            if status:
                return status
            return {"status": "not_found", "result": None}
        except Exception as e:
            logger.error(f"Error getting status for task {task_id}: {str(e)}")
            return {"status": "error", "result": str(e)}
  
   
          
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 client.py <topic>")
        sys.exit(1)

    topic = sys.argv[1]
    
    print("\nChecking required services...")
    if not check_services():
        print("\nError: Some required services are not running!")
        print("Please ensure both Redis and Kafka are running before starting the client.")
        sys.exit(1)

    try:
        print("\nInitializing client...")
        client = Client(topic=topic)
        
        print("\nSubmitting tasks...")
        task_ids = client.submit_tasks()
        
        if task_ids:
            print(f"\nSuccessfully submitted {len(task_ids)} tasks")
            print("\nMonitoring task execution...")
            results = client.monitor_tasks(task_ids)
        else:
            print("\nNo tasks were submitted successfully")
            
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)
