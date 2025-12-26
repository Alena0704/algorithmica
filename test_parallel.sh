#!/bin/bash

PROGRAM="./read_write_processes.c"
BINARY="./read_write_processes"
FILE="edit_parallel"
QUEUE_FILE="task_queue.dat"
ACTIVE_PROCESSES_FILE="active_processes.dat"
COMPLETED_TASKS_FILE="completed_tasks.dat"
TIMES=30

# Compile the program if it doesn't exist or source is newer
if [ ! -f "$BINARY" ] || [ "$PROGRAM" -nt "$BINARY" ]; then
    echo "Compiling $BINARY..."
    gcc -o "$BINARY" "$PROGRAM" -Wall -Wextra
    if [ $? -ne 0 ]; then
        echo "Compilation failed!"
        exit 1
    fi
    echo "Compilation successful!"
fi

# Create the file if it doesn't exist
touch "$FILE"

# Clean up old files
rm -f "$QUEUE_FILE" "${QUEUE_FILE}.tmp"
rm -f "$ACTIVE_PROCESSES_FILE" "${ACTIVE_PROCESSES_FILE}.tmp"
rm -f "$COMPLETED_TASKS_FILE" "${COMPLETED_TASKS_FILE}.tmp"
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
echo "=========================================="

# Run 100 operations
for i in $(seq 1 $TIMES); do
    # Generate random number (0 or 1)
    # 0 = read, 1 = write
    operation=$((RANDOM % 2))

    if [ $operation -eq 0 ]; then
        echo "[$i/$TIMES] Operation: READ"
        "$BINARY" read
        read_count=$((read_count + 1))
    else
        echo "[$i/$TIMES] Operation: WRITE (data: iteration_$i)"
        "$BINARY" write "iteration_$i"
        write_count=$((write_count + 1))
    fi

    # Small delay to make output more readable
    sleep 0.1
done

echo "=========================================="
echo "Completed $TIMES operations:"
echo "  READ operations:  $read_count"
echo "  WRITE operations: $write_count"
echo "=========================================="

# Wait for worker to process remaining tasks
echo "Waiting for worker to process remaining tasks..."
# Wait longer: TIMES * WORKER_SLEEP (2 seconds) + some buffer
sleep_time=$((TIMES * 3 + 5))
echo "Waiting ${sleep_time} seconds for worker to process all tasks..."

# Wait in smaller increments and check if queue is empty
for i in $(seq 1 $sleep_time); do
    sleep 1
    if [ ! -f "$QUEUE_FILE" ] || [ ! -s "$QUEUE_FILE" ]; then
        echo "Queue is empty, all tasks processed!"
        break
    fi
done

# Final check
if [ -f "$QUEUE_FILE" ] && [ -s "$QUEUE_FILE" ]; then
    echo "Warning: Queue file still contains tasks:"
    cat "$QUEUE_FILE"
fi

# Kill worker process
echo "Stopping worker process (PID: $WORKER_PID)..."
kill $WORKER_PID 2>/dev/null
wait $WORKER_PID 2>/dev/null

echo "=========================================="
echo "Log file: logfile.txt"
echo "Data file: $FILE"
echo "Queue file: $QUEUE_FILE"
echo "=========================================="

