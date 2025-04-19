# RR-Team-73-yadtq-yet-another-distributed-task-queue-
# Yet Another Distributed Task Queue (YADTQ)

## Overview
YADTQ is a robust, scalable distributed task queue system built with Python, leveraging Kafka for message queuing and Redis for task and worker management.

## System Components
- `yadtq.py`: Core task queue management class
- `worker.py`: Task processing worker 
- `client.py`: Task submission and monitoring client
- `backend.py`: Redis-based backend for task and worker tracking

## Key Features
- Distributed task processing
- Kafka-based message queuing
- Redis-powered task and worker tracking
- Automatic task requeuing on worker failure
- Worker heartbeat monitoring
- Task status tracking
- Flexible task types and argument handling

## Architecture
1. **Client** submits tasks with unique IDs to Kafka
2. **Workers** consume tasks from Kafka
3. **Backend (Redis)** tracks:
   - Task statuses
   - Worker heartbeats
   - Task assignments

## Fault Tolerance
- Automatic task requeuing if a worker fails
- Heartbeat monitoring detects worker disconnections
- Distributed locking prevents duplicate task processing

## Prerequisites
- Python 3.8+
- Kafka
- Redis
- Required Python packages: 
  - `kafka-python`
  - `redis`
  - `tabulate`

## Usage
1. Start Kafka and Redis
2. Launch workers in different terminals: 
   ```
   python worker.py worker1 tasks_topic
   python worker.py worker2 tasks_topic
   python worker.py worker3 tasks_topic
   ```
3. Run client:
   ```
   python client.py tasks_topic
   ```

## Supported Task Types
- Basic arithmetic: add, subtract, multiply
- Extensible architecture for custom task types

## Performance & Scalability
- Horizontal worker scaling
- Low-latency task processing
- Minimal overhead with efficient Redis and Kafka integration
