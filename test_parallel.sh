#!/bin/bash

PROGRAM="./read_write_processes.c"
BINARY="./read_write_processes"
FILE="edit_parallel"
COMPLETED_TASKS_FILE="completed_tasks.dat"
PLANNED_TASKS_FILE="planned_tasks.dat"
TIMES=30

# Compile the program
gcc -o "$BINARY" "$PROGRAM" -Wall -Wextra

# Clean up old files
rm -f task_queue.dat task_queue.dat.tmp
rm -f active_processes.dat active_processes.dat.tmp active_processes_shm.dat
rm -f "$COMPLETED_TASKS_FILE" "${COMPLETED_TASKS_FILE}.tmp"
rm -f "$PLANNED_TASKS_FILE" "${PLANNED_TASKS_FILE}.tmp"
rm -f "logfile.txt"
rm -f edit_parallel

# Start worker process in background
echo "Starting worker process..."
"$BINARY" worker &
WORKER_PID=$!
echo "Worker process started with PID: $WORKER_PID"

# Wait a bit for worker to initialize
sleep 1

# Initialize counters
read_count=0
write_count=0

echo "Starting $TIMES random read/write operations..."

# Run operations in parallel to create conflicts
for i in $(seq 1 $TIMES); do
    # Generate random number (0 or 1)
    # 0 = read, 1 = write
    operation=$((RANDOM % 2))

    if [ $operation -eq 0 ]; then
        echo "[$i/$TIMES] Operation: READ (background)"
        "$BINARY" read &
        read_count=$((read_count + 1))
    else
        echo "[$i/$TIMES] Operation: WRITE (background)"
        "$BINARY" write "iteration_$i" &
        write_count=$((write_count + 1))
    fi

    # Small delay between launching operations to create overlapping execution
    sleep 0.05
done

# Wait for all background processes to complete
echo "Waiting for all operations to complete..."
wait

echo "Completed $TIMES operations:"
echo "  READ operations:  $read_count"
echo "  WRITE operations: $write_count"

# Wait for worker to process remaining tasks and exit automatically
echo "Waiting for worker to process all tasks and exit automatically..."
sleep_time=$((TIMES * 3 + 10))
echo "Waiting up to ${sleep_time} seconds for worker to complete all tasks..."

# Wait for worker process to exit (it will exit when all tasks are completed)
for i in $(seq 1 $sleep_time); do
    if ! kill -0 $WORKER_PID 2>/dev/null; then
        echo "Worker process exited automatically (all tasks completed)."
        break
    fi
    sleep 1
done

# If worker is still running, kill it
if kill -0 $WORKER_PID 2>/dev/null; then
    echo "Warning: Worker process still running after timeout. Stopping it..."
    kill $WORKER_PID 2>/dev/null
    wait $WORKER_PID 2>/dev/null
else
    wait $WORKER_PID 2>/dev/null
fi

echo "Log file: logfile.txt"
echo "Data file: $FILE"

