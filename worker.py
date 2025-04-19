import sys
import time
import json
import signal
import logging
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from yadtq import YADTQ
import redis
from backend import Backend

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('Worker')

HEARTBEAT_INTERVAL=1

class Worker:
    def __init__(self, worker_id, topic):
        self.worker_id = worker_id
        self.topic = topic
        self.current_task_id = None
        self.current_task_lock = None
        try:
            self.redis_client = redis.StrictRedis(
                host='localhost', 
                port=6379, 
                db=0,
                socket_timeout=5
            )
            self.yadtq = YADTQ(topic=topic, backend=Backend(self.redis_client))
            self.running = True
            
            # Test Redis connection
            self.redis_client.ping()
            logger.info(f"Worker {worker_id} initialized successfully")
            
        except redis.ConnectionError as e:
            logger.error(f"Failed to connect to Redis: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Initialization error: {str(e)}")
            raise
        self.yadtq.backend.register_worker(self.worker_id)

    def start(self):
        """Start the worker with heartbeat monitoring"""
        logger.info(f"Starting worker {self.worker_id} on topic {self.topic}")
        
        try:
            import threading
            # Start heartbeat thread
            self.heartbeat_thread = threading.Thread(target=self._send_heartbeats)
            self.heartbeat_thread.daemon = True
            self.heartbeat_thread.start()
            logger.info("Heartbeat thread started")

            # Start processing tasks
            self._process_tasks()
            
        except Exception as e:
            logger.error(f"Error in worker start: {str(e)}")
            self.stop()
            raise

    def _send_heartbeats(self):
        """Send periodic heartbeats with better error handling"""
        logger.info(f"Started heartbeat monitoring for worker {self.worker_id}")
        consecutive_failures = 0
        max_failures = 3  # Maximum number of consecutive failures before stopping
    
        while self.running:
            try:
                self.yadtq.backend.update_worker_heartbeat(self.worker_id)
                consecutive_failures = 0  # Reset on successful heartbeat
                time.sleep(HEARTBEAT_INTERVAL)
            except Exception as e:
                logger.error(f"Error in heartbeat: {str(e)}")
                logger.critical(f"Too many consecutive heartbeat failures. Stopping worker.")
                self.stop()
                break
            time.sleep(1)

    def _process_tasks(self):
        """Process tasks from Kafka queue"""
        logger.info(f"Connecting to Kafka topic: {self.topic}")
        
        try:
            consumer = KafkaConsumer(
                self.topic,
                group_id=f"worker_group_{self.worker_id}",
                bootstrap_servers='localhost:9092',
                enable_auto_commit=False,
                auto_offset_reset='earliest',
                consumer_timeout_ms=1000  # 1 second timeout for polling
            )
            
            logger.info("Successfully connected to Kafka")
            
            while self.running:
                try:
                    # Poll for messages
                    message_batch = consumer.poll(timeout_ms=1000)
                    
                    if not message_batch:
                        continue
                        
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            self._handle_message(message, consumer)
                            
                except Exception as e:
                    logger.error(f"Error processing message batch: {str(e)}")
                    time.sleep(1)  # Wait before retry
                    
        except KafkaError as e:
            logger.error(f"Kafka error: {str(e)}")
            self.stop()
        except Exception as e:
            logger.error(f"Unexpected error in message processing loop: {str(e)}")
            self.stop()
        finally:
            logger.info("Closing Kafka consumer")
            consumer.close()

    def _handle_message(self, message, consumer):
        """Enhanced message handling with better failure tracking"""
        try:
            task_data = json.loads(message.value.decode('utf-8'))
            task_id = task_data["task_id"]
    
            logger.info(f"Processing task {task_id}")
    
            # Use Redis lock to ensure task uniqueness
            lock_key = f"task_lock:{task_id}"
            lock = self.redis_client.lock(
                lock_key,
                timeout=300,  # 5 minutes timeout
                blocking_timeout=5
            )
    
            if not lock.acquire(blocking=False):
                logger.info(f"Task {task_id} is already being processed")
                consumer.commit()
                return
    
            self.current_task_id = task_id
            self.current_task_lock = lock
    
            try:
                # Double-check task status after acquiring lock
                current_status = self.yadtq.backend.get_status(task_id)
                if current_status and current_status["status"] in ["processing", "success", "failed"]:
                    logger.info(f"Task {task_id} is already being processed or completed")
                    consumer.commit()
                    return
        
                # Update status to processing
                self.yadtq.backend.update_status(
                    task_id,
                    "processing",
                    worker_id=self.worker_id
                )
        
                # Process the task with enhanced error handling
                try:
                    result = self._execute_task(task_data)
                    status = "success"
                except Exception as e:
                    result = str(e)
                    status = "failed"
                    logger.error(f"Task {task_id} failed: {result}")
        
                # Update final status
                self.yadtq.backend.update_status(
                    task_id,
                    status,
                    result=result,
                    worker_id=self.worker_id
                )
        
                if status == "success":
                    logger.info(f"Successfully processed task {task_id}")
                else:
                    logger.warning(f"Task {task_id} failed during processing")
        
            finally:
                # Release the lock regardless of success or failure
                try:
                    if self.current_task_lock:
                        self.current_task_lock.release()
                        self.current_task_lock = None
                    self.current_task_id = None
                except:
                    pass
    
            consumer.commit()
    
        except ValueError as e:
            logger.error(f"Task {task_id} failed due to unknown task type")
            self.yadtq.backend.update_status(
                task_id,
                "failed",
                result=str(e),
                worker_id=self.worker_id
            )
        

    def _execute_task(self, task_data):
        """Execute the task and return result"""
        task = task_data["task"]
        args = task_data["args"]
        
        logger.info(f"Executing task {task} with args {args}")
        
        # Simulate some processing time
        time.sleep(2)

        if task == "add":
            return sum(args)
        elif task == "subtract":
            return args[0] - args[1]
        elif task == "multiply":
            return args[0] * args[1]
        else:
            raise ValueError(f"Unknown task type: {task}")

    def stop(self):
        """Stop the worker gracefully"""
        logger.info("Stopping worker gracefully...")
        self.running = False
        '''try: 
            self.yadtq.backend.mark_worker_dead(self.worker_id)
        except Exception as e:
            logger.error(f"Error during worker shutdown: {str(e)}")'''

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info("Received shutdown signal")
    if 'worker' in globals():
        worker.stop()
    sys.exit(0)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 worker.py <worker_name> <topic>")
        sys.exit(1)

    worker_name = sys.argv[1]
    topic = sys.argv[2]
    
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        logger.info(f"Initializing worker {worker_name} for topic {topic}")
        worker = Worker(worker_name, topic)
        worker.start()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
        worker.stop()
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)
