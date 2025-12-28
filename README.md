# Algorithmica

Realization of different interesting tasks.

## Reader-Writer Lock System with Task Queue Management

This project implements a file-based reader-writer lock system with two independent processes for managing concurrent file access.

### Architecture

The system consists of two independent processes:

1. **Task Manager** (`main` process): Accepts read/write tasks from command line and adds them to a queue file (`task_queue.dat`)
2. **Worker Process** (`worker` mode): Continuously processes tasks from the queue, managing concurrent access using reader-writer locks

### Features

- **Reader-Writer Lock Logic**:
  - `READ` tasks (SHARED_LOCK): Can run simultaneously with other READ tasks
  - `WRITE` tasks (EXCLUSIVE_LOCK): Block all other operations (read or write) and wait for completion

- **File-based Inter-Process Communication**: Uses `task_queue.dat` for task queuing between processes

- **Active Process Tracking**: Maintains `active_processes.dat` to track currently executing processes and prevent conflicts

- **Automatic Task Queuing**: Tasks that cannot acquire locks immediately are automatically queued and retried

- **Statistics Logging**: Tracks and logs:
  - Waiting tasks count
  - Active processes count
  - Completed tasks count

- **Rejection Logging**: Logs when read/write processes are blocked, including information about blocking processes

- **Random Delays**: Simulates real-world I/O operations with random delays (200-1000ms) for read/write operations

- **I/O**: Uses low-level file operations (`open`, `read`, `write`, `lseek`, `fsync`)

### Files

- `read_write_processes.c`: Main implementation with task manager and worker logic
- `test_parallel.sh`: Test script that runs 100 random read/write operations
- `logfile.txt`: Detailed operation logs with process states and file contents
- `task_queue.dat`: Queue file for inter-process communication
- `active_processes.dat`: Active process tracking file
- `completed_tasks.dat`: Completed tasks counter
- `edit_parallel`: Target file for read/write operations

### Usage

1. **Compile the program**:
   ```bash
   gcc -o read_write_processes read_write_processes.c -Wall -Wextra
   ```

2. **Start the worker process**:
   ```bash
   ./read_write_processes worker &
   ```

3. **Submit tasks** (from another terminal or script):
   ```bash
   ./read_write_processes read    # Submit a read task
   ./read_write_processes write   # Submit a write task
   ```

4. **Run test script**:
   ```bash
   ./test_parallel.sh
   ```

### How It Works

1. Task Manager receives a read/write request and adds it to `task_queue.dat`
2. Worker process continuously checks the queue file
3. For each task:
   - **READ task**: Checks if any WRITE process is active. If not, forks a child process to execute the read (can run simultaneously with other reads)
   - **WRITE task**: Checks if any process (read or write) is active. If not, acquires exclusive lock and executes the write
4. If a task cannot be executed immediately, it's put back in the queue
5. All operations are logged to `logfile.txt` with detailed information

### Logging

The system logs:
- Task submission and processing
- Lock acquisition and release
- Process forking and completion
- Task rejections with blocking process information
- File contents after each read/write operation
- Statistics (waiting, active, completed tasks)

### Example Output

```
Worker: Processing task for PID 1234 (type: READ)
Worker: Checking active processes for READ task PID 1234. Has write: 0. Active list: none
Forked read process 5678: Started reading for task PID 1234
Process 1234: Read 115 bytes from file
Process 1234: File contents (state after read): [file contents]
Forked read process 5678: Completed reading for task PID 1234

=== STATISTICS ===
Waiting tasks: 5
Active processes: 2
Completed tasks: 150
==================
```

### Requirements

- C compiler (gcc)
- POSIX-compliant system (Linux, macOS, etc.)
- Standard C library

### Notes

- The system uses file-based locking (`flock`) for synchronization
- Processes are tracked by PID and checked for liveness
- Dead processes are automatically cleaned up from the active processes list
- All file operations use low-level I/O with `fsync` for data integrity
