/*
 * Reader-Writer Lock System with Queue Management
 *
 * This program implements a file-based reader-writer lock system with two independent processes:
 *
 * 1. Task Manager (main process): Accepts read/write tasks from command line and adds them to a queue file.
 * 2. Worker Process: Continuously processes tasks from the queue, managing concurrent access:
 *    - READ tasks (SHARED_LOCK): Can run simultaneously with other READ tasks, but blocked by WRITE tasks.
 *    - WRITE tasks (EXCLUSIVE_LOCK): Block all other operations (read or write) and wait for completion.
 *
 * Features:
 * - File-based inter-process communication via task_queue.dat
 * - Active process tracking to prevent conflicts
 * - Automatic task queuing when locks cannot be acquired
 * - Statistics logging (waiting, active, completed tasks)
 * - Random delays (200-1000ms) for read/write operations
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <signal.h>

#define FILENAME "edit_parallel"
#define QUEUE_FILE "task_queue.dat"
#define QUEUE_LOCK_FILE "task_queue.lock"
#define ACTIVE_PROCESSES_FILE "active_processes.dat"
#define COMPLETED_TASKS_FILE "completed_tasks.dat"
#define WORKER_SLEEP 1  /* seconds between retry attempts - reduced to check more frequently */
#define MIN_DELAY_MS 200
#define MAX_DELAY_MS 1000
#define MAX_DATA_SIZE 1024

typedef enum {
    SHARED_LOCK = 0,
    EXCLUSIVE_LOCK = 1,
    INVALID_LOCK = 2
} LockType;

// Structure to store lock information
typedef struct {
    int fd;
    LockType lock_type;
} LockInfo;

/*
 * Assert
 *		Generates a fatal exception if the given condition is false.
 */
#define Assert(condition) \
do { \
    if (!(condition)) { \
        fprintf(stderr, "Assertion failed: %s\n", #condition); \
        exit(1); \
    } \
} while (0)

/**
 * Try to acquire lock in non-blocking mode
 * Returns NULL if lock cannot be acquired immediately
 */
LockInfo* try_acquire_lock_nonblocking(LockType lock_type)
{
    LockInfo* lock_info;
    int flags;

    lock_info = malloc(sizeof(LockInfo));
    if (lock_info == NULL || lock_type == INVALID_LOCK)
    {
        fprintf(stderr, "Fatal error: Failed to allocate lock info\n");
        exit(1);
    }

    lock_info->lock_type = lock_type;

    /* Open file with appropriate flags */
    if (lock_type == SHARED_LOCK)
    {
        flags = O_RDONLY | O_CREAT;
    }
    else
    {
        /* Use O_RDWR to allow reading back after write */
        flags = O_RDWR | O_CREAT | O_TRUNC;
    }

    lock_info->fd = open(FILENAME, flags, 0644);
    if (lock_info->fd < 0)
    {
        free(lock_info);
        return NULL;
    }

    /* Try to acquire file lock in non-blocking mode */
    if (lock_type == SHARED_LOCK)
    {
        if (flock(lock_info->fd, LOCK_SH | LOCK_NB) < 0)
        {
            close(lock_info->fd);
            free(lock_info);
            return NULL;  /* Lock not available */
        }
    }
    else
    {
        if (flock(lock_info->fd, LOCK_EX | LOCK_NB) < 0)
        {
            close(lock_info->fd);
            free(lock_info);
            return NULL;  /* Lock not available */
        }
    }

    return lock_info;
}

/*
 * Release lock
 */
void release_lock_info(LockInfo* lock_info)
{
    Assert(lock_info != NULL);

    if (flock(lock_info->fd, LOCK_UN) < 0)
    {
        fprintf(stderr, "Fatal error: Failed to release %s lock\n", lock_info->lock_type == SHARED_LOCK ? "SHARED" : "EXCLUSIVE");
        exit(1);
    }

    Assert(lock_info->fd >= 0);

    if (close(lock_info->fd) != 0)
    {
        fprintf(stderr, "Fatal error: Failed to close file\n");
        exit(1);
    }

    free(lock_info);
}

void stream_write_logfile(const char* message)
{
    if (message == NULL)
    {
        return;
    }

    int logfile = open("logfile.txt", O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (logfile >= 0)
    {
        size_t msg_len = strlen(message);
        if (msg_len > 0)
        {
            write(logfile, message, msg_len);
        }
        close(logfile);
        return;
    }
    /* Ignore errors - logging should not fail the program */
}

/* Generate random delay between min_ms and max_ms in milliseconds */
void random_delay_ms(int min_ms, int max_ms)
{
    int delay_ms;
    struct timespec ts;

    if (max_ms <= min_ms)
    {
        delay_ms = min_ms;
    }
    else
    {
        delay_ms = min_ms + (rand() % (max_ms - min_ms + 1));
    }

    ts.tv_sec = delay_ms / 1000;
    ts.tv_nsec = (delay_ms % 1000) * 1000000;
    nanosleep(&ts, NULL);
}

void read_from_file(pid_t pid, LockInfo* lock)
{
    char buffer[1024];
    char read_data[2048] = {0};
    ssize_t bytes_read;
    char log_message[4096];
    int total_read = 0;

    if (lock == NULL || lock->fd < 0)
    {
        fprintf(stderr, "Fatal error: Invalid lock in read_from_file\n");
        return;
    }

    /* Random delay before reading (10-100 ms) */
    random_delay_ms(MIN_DELAY_MS, MAX_DELAY_MS);

    /* Seek to beginning to read entire file */
    lseek(lock->fd, 0, SEEK_SET);

    while ((bytes_read = read(lock->fd, buffer, sizeof(buffer) - 1)) > 0)
    {
        buffer[bytes_read] = '\0';
        if (total_read + (int)bytes_read < (int)(sizeof(read_data) - 1))
        {
            strcat(read_data, buffer);
            total_read += (int)bytes_read;
        }
    }

    if (total_read > 0)
    {
        /* Log what was read */
        snprintf(log_message, sizeof(log_message),
                 "Process %d: Read %d bytes from file\n", (int)pid, total_read);
        stream_write_logfile(log_message);

        /* Log file contents (state of file after read) */
        snprintf(log_message, sizeof(log_message),
                 "Process %d: File contents (state after read): %s\n",
                 (int)pid, read_data);
        stream_write_logfile(log_message);
    }
    else
    {
        snprintf(log_message, sizeof(log_message),
                 "Process %d: File is empty or read failed\n", (int)pid);
        stream_write_logfile(log_message);

        /* Log empty file state */
        snprintf(log_message, sizeof(log_message),
                 "Process %d: File contents (state after read): [FILE IS EMPTY]\n",
                 (int)pid);
        stream_write_logfile(log_message);
    }
}

char* random_data()
{
    char* data;
    int len = 100;  /* Reasonable length */

    data = (char*)malloc(len + 1);
    if (data == NULL)
    {
        fprintf(stderr, "Fatal error: Failed to allocate memory for random data\n");
        exit(1);
    }

    for (int i = 0; i < len; i++)
        data[i] = 'a' + rand() % 26;
    data[len] = '\0';  /* Null terminator */
    return data;
}

void write_to_file(pid_t pid, char* write_data, LockInfo* lock)
{
    ssize_t bytes_written;
    char log_message[4096];
    char write_buffer[2048];
    char file_contents[2048] = {0};
    ssize_t read_bytes;

    if (lock == NULL || lock->fd < 0)
    {
        fprintf(stderr, "Fatal error: Invalid lock in write_to_file\n");
        return;
    }

    if (write_data == NULL)
    {
        fprintf(stderr, "Fatal error: write_data is NULL\n");
        return;
    }

    /* Random delay before writing (10-100 ms) */
    random_delay_ms(MIN_DELAY_MS, MAX_DELAY_MS);

    /* Prepare write buffer */
    snprintf(write_buffer, sizeof(write_buffer), "Process %d: %s\n", (int)pid, write_data);

    /* Log what will be written */
    snprintf(log_message, sizeof(log_message),
             "Process %d: Writing data: %s", (int)pid, write_buffer);
    stream_write_logfile(log_message);

    /* Write to file using low-level write() - PostgreSQL style */
    bytes_written = write(lock->fd, write_buffer, strlen(write_buffer));
    if (bytes_written < 0) {
        fprintf(stderr, "Fatal error: write failed\n");
        return;
    } else {
        /* Ensure data is written to disk (like fsync in PostgreSQL) */
        fsync(lock->fd);

        /* Log successful write */
        snprintf(log_message, sizeof(log_message),
                 "Process %d: Successfully wrote %zd bytes\n", (int)pid, bytes_written);
        stream_write_logfile(log_message);

        /* Read back file contents to verify and log */
        /* Reset file position to beginning */
        if (lseek(lock->fd, 0, SEEK_SET) < 0)
        {
            perror("lseek failed");
        }
        else
        {
            read_bytes = read(lock->fd, file_contents, sizeof(file_contents) - 1);
            if (read_bytes > 0)
            {
                file_contents[read_bytes] = '\0';
            }
            else if (read_bytes == 0)
            {
                strcpy(file_contents, "[FILE IS EMPTY]");
            }
            else
            {
                strcpy(file_contents, "[READ ERROR]");
            }

            /* Log file contents (state of file after write) */
            snprintf(log_message, sizeof(log_message),
                     "Process %d: File contents (state after write): %s\n",
                     (int)pid, file_contents);
            stream_write_logfile(log_message);
        }

        printf("Process %d: Wrote %zd bytes: %s", (int)pid, bytes_written, write_buffer);
    }
}

/* ------------------------------------------------------------
 * List implementation
 * Queue node is an element of waiting processes list in the queue
 *------------------------------------------------------------ */

/* Queue node structure for List */
struct QueueNode {
    pid_t pid;
    LockType lock_type;
    char* data;

    struct QueueNode* next;
    struct QueueNode* prev;
};

/* Forward declaration */
typedef struct QueueNode QueueNode;

/* Queue structure for List */
typedef struct QueueList {
    struct QueueNode* head;
    struct QueueNode* tail;
    int size;
} QueueList;

/* Initialize the queue */
void init_queue(QueueList* queue)
{
    queue->head = NULL;
    queue->tail = NULL;
    queue->size = 0;
}

/* Add process to the end of the list */
int queue_list_append(QueueList* list, struct QueueNode* node)
{
    struct QueueNode* current;

    Assert(list != NULL);
    Assert(node != NULL);

    if(list->size == 0)
    {
        list->head = node;
        list->tail = node;
        node->next = NULL;
        node->prev = NULL;
    }
    else
    {
        current = list->tail;
        current->next = node;
        node->prev = current;
        node->next = NULL;
        list->tail = node;
    }

    list->size++;

    return 0;
}

/* Get and remove first node from the list */
struct QueueNode* queue_list_pop_first(QueueList* list)
{
    struct QueueNode* node;

    if (list == NULL || list->size == 0)
    {
        return NULL;
    }

    node = list->head;
    if (node == NULL)
    {
        return NULL;
    }

    list->head = node->next;
    if (list->head != NULL)
    {
        list->head->prev = NULL;
    }
    else
    {
        list->tail = NULL;
    }

    list->size--;

    return node;
}

/* Check if there are active processes working with file */
int has_active_write_process()
{
    FILE* active_file;
    char line[256];
    int file_pid;
    int lock_type_int;
    int has_write = 0;

    active_file = fopen(ACTIVE_PROCESSES_FILE, "r");
    if (active_file == NULL)
    {
        return 0;  /* No active processes file, no active writes */
    }

    while (fgets(line, sizeof(line), active_file) != NULL)
    {
        if (sscanf(line, "%d:%d", &file_pid, &lock_type_int) == 2)
        {
            /* Check if process is still alive */
            if (kill((pid_t)file_pid, 0) == 0)
            {
                if (lock_type_int == EXCLUSIVE_LOCK)
                {
                    has_write = 1;
                    break;
                }
            }
        }
    }

    fclose(active_file);
    return has_write;
}

int has_any_active_process()
{
    FILE* active_file;
    char line[256];
    int file_pid;
    int lock_type_int;
    int has_active = 0;

    active_file = fopen(ACTIVE_PROCESSES_FILE, "r");
    if (active_file == NULL)
    {
        return 0;  /* No active processes */
    }

    while (fgets(line, sizeof(line), active_file) != NULL)
    {
        if (sscanf(line, "%d:%d", &file_pid, &lock_type_int) == 2)
        {
            /* Check if process is still alive */
            if (kill((pid_t)file_pid, 0) == 0)
            {
                has_active = 1;
                break;
            }
        }
    }

    fclose(active_file);
    return has_active;
}

/* Add process to active processes file */
int add_active_process(pid_t pid, LockType lock_type)
{
    FILE* active_file;
    int active_fd;

    active_fd = open(ACTIVE_PROCESSES_FILE, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (active_fd < 0)
    {
        return -1;
    }

    if (flock(active_fd, LOCK_EX) < 0)
    {
        close(active_fd);
        return -1;
    }

    active_file = fdopen(active_fd, "a");
    if (active_file == NULL)
    {
        flock(active_fd, LOCK_UN);
        close(active_fd);
        return -1;
    }

    fprintf(active_file, "%d:%d\n", (int)pid, (int)lock_type);
    fflush(active_file);

    fclose(active_file);
    flock(active_fd, LOCK_UN);
    close(active_fd);

    return 0;
}

/* Remove process from active processes file */
int remove_active_process(pid_t pid)
{
    FILE* active_file;
    FILE* temp_file;
    int active_fd;
    char line[256];
    char temp_filename[256];
    int file_pid;
    int lock_type_int;
    int found = 0;

    snprintf(temp_filename, sizeof(temp_filename), "%s.tmp", ACTIVE_PROCESSES_FILE);

    active_fd = open(ACTIVE_PROCESSES_FILE, O_RDWR | O_CREAT, 0644);
    if (active_fd < 0)
    {
        return -1;
    }

    if (flock(active_fd, LOCK_EX) < 0)
    {
        close(active_fd);
        return -1;
    }

    active_file = fdopen(active_fd, "r+");
    if (active_file == NULL)
    {
        flock(active_fd, LOCK_UN);
        close(active_fd);
        return -1;
    }

    temp_file = fopen(temp_filename, "w");
    if (temp_file != NULL)
    {
        while (fgets(line, sizeof(line), active_file) != NULL)
        {
            if (sscanf(line, "%d:%d", &file_pid, &lock_type_int) == 2)
            {
                if (file_pid != (int)pid)
                {
                    /* Keep this process, check if still alive */
                    if (kill((pid_t)file_pid, 0) == 0)
                    {
                        fputs(line, temp_file);
                    }
                }
                else
                {
                    found = 1;
                }
            }
        }
        fclose(temp_file);
    }

    fclose(active_file);
    flock(active_fd, LOCK_UN);
    close(active_fd);

    if (found)
    {
        rename(temp_filename, ACTIVE_PROCESSES_FILE);
    }
    else
    {
        unlink(temp_filename);
    }

    return found ? 0 : -1;
}

/* Clean up stale processes from active processes file */
void cleanup_active_processes()
{
    FILE* active_file;
    FILE* temp_file;
    int active_fd;
    char line[256];
    char temp_filename[256];
    int file_pid;
    int lock_type_int;

    snprintf(temp_filename, sizeof(temp_filename), "%s.tmp", ACTIVE_PROCESSES_FILE);

    active_fd = open(ACTIVE_PROCESSES_FILE, O_RDWR | O_CREAT, 0644);
    if (active_fd < 0)
    {
        return;
    }

    if (flock(active_fd, LOCK_EX) < 0)
    {
        close(active_fd);
        return;
    }

    active_file = fdopen(active_fd, "r+");
    if (active_file == NULL)
    {
        flock(active_fd, LOCK_UN);
        close(active_fd);
        return;
    }

    temp_file = fopen(temp_filename, "w");
    if (temp_file != NULL)
    {
        while (fgets(line, sizeof(line), active_file) != NULL)
        {
            if (sscanf(line, "%d:%d", &file_pid, &lock_type_int) == 2)
            {
                /* Check if process is still alive */
                if (kill((pid_t)file_pid, 0) == 0)
                {
                    fputs(line, temp_file);
                }
            }
        }
        fclose(temp_file);
    }

    fclose(active_file);
    flock(active_fd, LOCK_UN);
    close(active_fd);

    rename(temp_filename, ACTIVE_PROCESSES_FILE);
}

/* Count tasks in queue file */
int count_queue_tasks()
{
    FILE* queue_file;
    int queue_fd;
    char line[4096];
    int count = 0;

    queue_fd = open(QUEUE_FILE, O_RDONLY | O_CREAT, 0644);
    if (queue_fd < 0)
    {
        return 0;
    }

    if (flock(queue_fd, LOCK_SH) < 0)
    {
        close(queue_fd);
        return 0;
    }

    queue_file = fdopen(queue_fd, "r");
    if (queue_file != NULL)
    {
        while (fgets(line, sizeof(line), queue_file) != NULL)
        {
            /* Count non-empty lines that look like tasks (PID:LOCK_TYPE:DATA) */
            if (strlen(line) > 0 && line[0] != '\n')
            {
                int dummy_pid, dummy_lock;
                /* Check if line matches task format - just check for PID:LOCK_TYPE: */
                if (sscanf(line, "%d:%d:", &dummy_pid, &dummy_lock) == 2)
                {
                    count++;
                }
            }
        }
        fclose(queue_file);
    }
    else
    {
        flock(queue_fd, LOCK_UN);
        close(queue_fd);
        return 0;
    }

    flock(queue_fd, LOCK_UN);
    close(queue_fd);

    return count;
}

/* Count active processes */
int count_active_processes()
{
    FILE* active_file;
    char line[256];
    int file_pid;
    int lock_type_int;
    int count = 0;

    active_file = fopen(ACTIVE_PROCESSES_FILE, "r");
    if (active_file == NULL)
    {
        return 0;
    }

    while (fgets(line, sizeof(line), active_file) != NULL)
    {
        if (sscanf(line, "%d:%d", &file_pid, &lock_type_int) == 2)
        {
            /* Check if process is still alive */
            if (kill((pid_t)file_pid, 0) == 0)
            {
                count++;
            }
        }
    }

    fclose(active_file);
    return count;
}

/* Count active read processes only */
int count_active_read_processes()
{
    FILE* active_file;
    char line[256];
    int file_pid;
    int lock_type_int;
    int count = 0;

    active_file = fopen(ACTIVE_PROCESSES_FILE, "r");
    if (active_file == NULL)
    {
        return 0;
    }

    while (fgets(line, sizeof(line), active_file) != NULL)
    {
        if (sscanf(line, "%d:%d", &file_pid, &lock_type_int) == 2)
        {
            /* Check if process is still alive and is a read process */
            if (kill((pid_t)file_pid, 0) == 0 && lock_type_int == SHARED_LOCK)
            {
                count++;
            }
        }
    }

    fclose(active_file);
    return count;
}

/* Get list of active processes as string (for logging) */
void get_active_processes_list(char* buffer, size_t buffer_size)
{
    FILE* active_file;
    char line[256];
    int file_pid;
    int lock_type_int;
    int first = 1;
    size_t pos = 0;

    buffer[0] = '\0';

    active_file = fopen(ACTIVE_PROCESSES_FILE, "r");
    if (active_file == NULL)
    {
        strncpy(buffer, "none", buffer_size - 1);
        buffer[buffer_size - 1] = '\0';
        return;
    }

    while (fgets(line, sizeof(line), active_file) != NULL)
    {
        if (sscanf(line, "%d:%d", &file_pid, &lock_type_int) == 2)
        {
            /* Check if process is still alive */
            if (kill((pid_t)file_pid, 0) == 0)
            {
                if (!first)
                {
                    if (pos < buffer_size - 1)
                    {
                        buffer[pos++] = ',';
                        buffer[pos++] = ' ';
                    }
                }
                first = 0;

                const char* type_str = (lock_type_int == SHARED_LOCK) ? "READ" : "WRITE";
                int written = snprintf(buffer + pos, buffer_size - pos, "PID %d (%s)", file_pid, type_str);
                if (written > 0 && (size_t)written < buffer_size - pos)
                {
                    pos += (size_t)written;
                }
            }
        }
    }

    fclose(active_file);

    if (pos == 0)
    {
        strncpy(buffer, "none", buffer_size - 1);
        buffer[buffer_size - 1] = '\0';
    }
    else
    {
        buffer[pos] = '\0';
    }
}

/* Get completed tasks count */
int get_completed_tasks_count()
{
    FILE* completed_file;
    int count = 0;

    completed_file = fopen(COMPLETED_TASKS_FILE, "r");
    if (completed_file != NULL)
    {
        if (fscanf(completed_file, "%d", &count) != 1)
        {
            count = 0;
        }
        fclose(completed_file);
    }

    return count;
}

/* Increment completed tasks count */
void increment_completed_tasks()
{
    FILE* completed_file;
    int queue_fd;
    int count = get_completed_tasks_count();

    queue_fd = open(COMPLETED_TASKS_FILE, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (queue_fd < 0)
    {
        return;
    }

    if (flock(queue_fd, LOCK_EX) < 0)
    {
        close(queue_fd);
        return;
    }

    completed_file = fdopen(queue_fd, "w");
    if (completed_file != NULL)
    {
        fprintf(completed_file, "%d\n", count + 1);
        fflush(completed_file);
        fclose(completed_file);
    }

    flock(queue_fd, LOCK_UN);
    close(queue_fd);
}

/* Print statistics */
void print_statistics()
{
    int waiting = 0;
    int active = 0;
    int completed = 0;
    char log_message[4096];

    /* Safely get statistics - ignore errors */
    waiting = count_queue_tasks();
    active = count_active_processes();
    completed = get_completed_tasks_count();

    snprintf(log_message, sizeof(log_message),
             "=== STATISTICS ===\n"
             "Waiting tasks: %d\n"
             "Active processes: %d\n"
             "Completed tasks: %d\n"
             "==================\n",
             waiting, active, completed);
    stream_write_logfile(log_message);
}

/* Add task to queue file (used by task manager process) */
int add_task_to_queue_file(pid_t pid, LockType lock_type, const char* data)
{
    FILE* queue_file;
    int queue_fd;
    char log_message[4096];

    /* Open queue file with exclusive lock */
    queue_fd = open(QUEUE_FILE, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (queue_fd < 0)
    {
        perror("Failed to open queue file");
        return -1;
    }

    /* Lock file exclusively */
    if (flock(queue_fd, LOCK_EX) < 0)
    {
        perror("Failed to lock queue file");
        close(queue_fd);
        return -1;
    }

    queue_file = fdopen(queue_fd, "a");
    if (queue_file == NULL)
    {
        flock(queue_fd, LOCK_UN);
        close(queue_fd);
        return -1;
    }

    /* Write task to queue file: PID:LOCK_TYPE:DATA */
    fprintf(queue_file, "%d:%d:%s\n", (int)pid, (int)lock_type, data != NULL ? data : "");
    fflush(queue_file);

    fclose(queue_file);
    flock(queue_fd, LOCK_UN);
    close(queue_fd);

    /* Log that task was added to queue */
    snprintf(log_message, sizeof(log_message),
             "Task Manager: Task added to queue (PID %d, type: %s)\n",
             (int)pid, lock_type == SHARED_LOCK ? "READ" : "WRITE");
    stream_write_logfile(log_message);

    printf("Task Manager: Task added to queue (PID %d, type: %s)\n",
           (int)pid, lock_type == SHARED_LOCK ? "READ" : "WRITE");

    /* Don't print statistics here to avoid potential conflicts with file locks */
    /* Statistics will be printed by worker process periodically */

    return 0;
}

/* Get and remove first task from queue file (used by worker process) */
int get_task_from_queue_file(pid_t* pid, LockType* lock_type, char* data, size_t data_size)
{
    FILE* queue_file;
    FILE* temp_file;
    int queue_fd;
    char line[4096];
    char temp_filename[256];
    int found = 0;
    int file_pid;
    int lock_type_int;
    char task_data[MAX_DATA_SIZE];

    snprintf(temp_filename, sizeof(temp_filename), "%s.tmp", QUEUE_FILE);

    /* Check if queue file exists and has content */
    queue_fd = open(QUEUE_FILE, O_RDWR | O_CREAT, 0644);
    if (queue_fd < 0)
    {
        return -1;  /* File doesn't exist or can't be opened */
    }

    if (flock(queue_fd, LOCK_EX) < 0)
    {
        close(queue_fd);
        return -1;
    }

    /* Check file size */
    off_t file_size = lseek(queue_fd, 0, SEEK_END);
    if (file_size <= 0)
    {
        flock(queue_fd, LOCK_UN);
        close(queue_fd);
        return -1;  /* File is empty */
    }

    /* Seek back to beginning */
    if (lseek(queue_fd, 0, SEEK_SET) < 0)
    {
        flock(queue_fd, LOCK_UN);
        close(queue_fd);
        return -1;
    }

    queue_file = fdopen(queue_fd, "r+");
    if (queue_file == NULL)
    {
        flock(queue_fd, LOCK_UN);
        close(queue_fd);
        return -1;
    }

    /* Read first line */
    if (fgets(line, sizeof(line), queue_file) != NULL)
    {
        /* Clear task_data first */
        memset(task_data, 0, MAX_DATA_SIZE);

        /* Parse line: PID:LOCK_TYPE:DATA or PID:LOCK_TYPE: */
        if (sscanf(line, "%d:%d:", &file_pid, &lock_type_int) == 2)
        {
            *pid = (pid_t)file_pid;
            *lock_type = (LockType)lock_type_int;

            /* Extract data part (everything after second colon) */
            char* first_colon = strchr(line, ':');
            if (first_colon != NULL)
            {
                char* second_colon = strchr(first_colon + 1, ':');
                if (second_colon != NULL)
                {
                    second_colon++;  /* Skip the colon */
                    /* Remove trailing newline if present */
                    char* newline = strchr(second_colon, '\n');
                    if (newline != NULL)
                    {
                        *newline = '\0';
                    }
                    /* Copy data, ensuring null termination */
                    size_t data_len = strlen(second_colon);
                    if (data_len > 0 && data_len < MAX_DATA_SIZE)
                    {
                        strncpy(task_data, second_colon, MAX_DATA_SIZE - 1);
                        task_data[MAX_DATA_SIZE - 1] = '\0';
                    }
                    else
                    {
                        task_data[0] = '\0';
                    }
                }
                else
                {
                    task_data[0] = '\0';
                }
            }
            else
            {
                task_data[0] = '\0';
            }

            if (data != NULL && data_size > 0)
            {
                strncpy(data, task_data, data_size - 1);
                data[data_size - 1] = '\0';
            }
            found = 1;
        }
    }

    if (found)
    {
        /* Rewind and copy remaining lines to temp file (skip first line) */
        rewind(queue_file);
        temp_file = fopen(temp_filename, "w");
        if (temp_file != NULL)
        {
            int first_line = 1;
            int has_remaining = 0;
            while (fgets(line, sizeof(line), queue_file) != NULL)
            {
                if (!first_line)
                {
                    fputs(line, temp_file);
                    has_remaining = 1;
                }
                first_line = 0;
            }
            fclose(temp_file);

            fclose(queue_file);
            flock(queue_fd, LOCK_UN);
            close(queue_fd);

            /* Replace queue file with temp file (or remove if empty) */
            if (has_remaining)
            {
                if (rename(temp_filename, QUEUE_FILE) < 0)
                {
                    unlink(temp_filename);
                }
            }
            else
            {
                /* No remaining tasks, remove queue file */
                unlink(temp_filename);
                unlink(QUEUE_FILE);
            }
        }
        else
        {
            fclose(queue_file);
            flock(queue_fd, LOCK_UN);
            close(queue_fd);
            return -1;
        }
    }
    else
    {
        fclose(queue_file);
        flock(queue_fd, LOCK_UN);
        close(queue_fd);
    }

    return found ? 0 : -1;
}

/* Execute read task in forked process */
void execute_read_task(pid_t task_pid)
{
    pid_t child_pid = fork();

    if (child_pid < 0)
    {
        perror("fork failed for read task");
        return;
    }

    if (child_pid == 0)
    {
        /* Child process - execute read */
        LockInfo* lock;
        char log_message[4096];
        pid_t my_pid = getpid();

        /* Add to active processes */
        add_active_process(my_pid, SHARED_LOCK);

        /* Count active read processes (including this one) */
        int active_reads = count_active_read_processes();

        snprintf(log_message, sizeof(log_message),
                 "Forked read process %d: Started reading for task PID %d (running simultaneously with %d other active read process(es))\n",
                 (int)my_pid, (int)task_pid, active_reads > 0 ? active_reads - 1 : 0);
        stream_write_logfile(log_message);

        lock = try_acquire_lock_nonblocking(SHARED_LOCK);
        if (lock != NULL)
        {
            read_from_file(task_pid, lock);
            release_lock_info(lock);
            remove_active_process(my_pid);

            /* Increment completed tasks counter */
            increment_completed_tasks();

            snprintf(log_message, sizeof(log_message),
                     "Forked read process %d: Completed reading for task PID %d\n",
                     (int)my_pid, (int)task_pid);
            stream_write_logfile(log_message);

            exit(0);
        }
        else
        {
            remove_active_process(my_pid);

            snprintf(log_message, sizeof(log_message),
                     "Forked read process %d: Failed to acquire lock for task PID %d\n",
                     (int)my_pid, (int)task_pid);
            stream_write_logfile(log_message);

            exit(1);
        }
    }
    /* Parent continues - don't wait for child, let it run independently */
    /* Use waitpid with WNOHANG to avoid blocking */
    waitpid(child_pid, NULL, WNOHANG);
}

/* Worker process - processes tasks from queue file */
void worker_process()
{
    pid_t task_pid;
    LockType lock_type;
    char task_data[MAX_DATA_SIZE];
    LockInfo* lock;
    char log_message[4096];
    char* write_data = NULL;

    printf("Worker process %d: Started, processing queue...\n", (int)getpid());

    while (1)
    {
        /* Clean up stale processes periodically */
        static int cleanup_count = 0;
        cleanup_count++;
        if (cleanup_count % 20 == 0)
        {
            cleanup_active_processes();
        }

        /* Print statistics periodically */
        static int stats_count = 0;
        stats_count++;
        if (stats_count % 5 == 0)
        {
            print_statistics();
        }

        /* Try to get task from queue file */
        memset(task_data, 0, sizeof(task_data));
        int result = get_task_from_queue_file(&task_pid, &lock_type, task_data, sizeof(task_data));
        if (result == 0)
        {
            /* Log that worker is processing task */
            snprintf(log_message, sizeof(log_message),
                     "Worker: Processing task for PID %d (type: %s)\n",
                     (int)task_pid, lock_type == SHARED_LOCK ? "READ" : "WRITE");
            stream_write_logfile(log_message);

            if (lock_type == SHARED_LOCK)
            {
                /* For READ: if no active write process, fork and execute */
                char active_list_check[2048];
                get_active_processes_list(active_list_check, sizeof(active_list_check));
                int has_write = has_active_write_process();

                /* Log check result */
                snprintf(log_message, sizeof(log_message),
                         "Worker: Checking active processes for READ task PID %d. Has write: %d. Active list: %s\n",
                         (int)task_pid, has_write, active_list_check);
                stream_write_logfile(log_message);

                /* Re-check multiple times with delays to catch WRITE processes that are active */
                /* This is important because worker processes tasks sequentially, and WRITE might */
                /* be active when we check, but finish before we process the READ task */
                int recheck_count = 0;
                int max_rechecks = 5;  /* Check up to 5 times */
                while (!has_write && recheck_count < max_rechecks)
                {
                    usleep(100000);  /* 100ms delay between checks */
                    has_write = has_active_write_process();
                    if (has_write)
                    {
                        /* Re-get active list after delay */
                        get_active_processes_list(active_list_check, sizeof(active_list_check));
                        snprintf(log_message, sizeof(log_message),
                                 "Worker: Re-checked active processes for READ task PID %d after delay (attempt %d). Has write: %d. Active list: %s\n",
                                 (int)task_pid, recheck_count + 1, has_write, active_list_check);
                        stream_write_logfile(log_message);
                        break;  /* Found active write, stop checking */
                    }
                    recheck_count++;
                }

                if (!has_write)
                {
                    /* Check how many active read processes exist before forking */
                    int active_reads_before = count_active_read_processes();

                    /* No active writes, can fork read process */
                    execute_read_task(task_pid);

                    /* Small delay to allow forked process to register */
                    usleep(50000);  /* 50ms delay to allow forked process to register */

                    /* Re-check active processes after forking */
                    char active_list_after_fork[2048];
                    get_active_processes_list(active_list_after_fork, sizeof(active_list_after_fork));
                    int waiting_count = count_queue_tasks();

                    snprintf(log_message, sizeof(log_message),
                             "Worker: Forked read process for PID %d (can run simultaneously with %d other active read process(es)). Current active processes: %s. Waiting tasks: %d\n",
                             (int)task_pid, active_reads_before, active_list_after_fork, waiting_count);
                    stream_write_logfile(log_message);
                }
                else
                {
                    /* Active write process, put back to queue */
                    add_task_to_queue_file(task_pid, lock_type, task_data);

                    snprintf(log_message, sizeof(log_message),
                             "Worker: READ task for PID %d rejected - active write process detected, put back to queue. Blocking processes: %s\n",
                             (int)task_pid, active_list_check);
                    stream_write_logfile(log_message);
                }
            }
            else
            {
                /* For WRITE: check if any process is active */
                char active_list_check[2048];
                get_active_processes_list(active_list_check, sizeof(active_list_check));
                int has_active = has_any_active_process();

                /* Log check result */
                snprintf(log_message, sizeof(log_message),
                         "Worker: Checking active processes for WRITE task PID %d. Has active: %d. Active list: %s\n",
                         (int)task_pid, has_active, active_list_check);
                stream_write_logfile(log_message);

                if (!has_active)
                {
                    /* No active processes, try to acquire exclusive lock */
                    lock = try_acquire_lock_nonblocking(EXCLUSIVE_LOCK);
                    if (lock != NULL)
                    {
                        /* Add to active processes BEFORE executing */
                        add_active_process(getpid(), EXCLUSIVE_LOCK);

                        /* Log that we added ourselves to active processes */
                        snprintf(log_message, sizeof(log_message),
                                 "Worker: Added WRITE process PID %d to active processes\n",
                                 (int)getpid());
                        stream_write_logfile(log_message);

                        /* Check if there are other tasks waiting that would be blocked */
                        /* Re-check active processes after adding ourselves */
                        char active_list_after[2048];
                        get_active_processes_list(active_list_after, sizeof(active_list_after));
                        int waiting_count = count_queue_tasks();

                        if (waiting_count > 0)
                        {
                            snprintf(log_message, sizeof(log_message),
                                     "Worker: WRITE process PID %d is now active. %d tasks waiting in queue. Active processes: %s\n",
                                     (int)getpid(), waiting_count, active_list_after);
                            stream_write_logfile(log_message);
                        }

                        /* Successfully acquired lock, execute task */
                        /* For write operations, generate random data if task_data is empty */
                        if (strlen(task_data) > 0)
                        {
                            write_data = strdup(task_data);
                        }
                        else
                        {
                            write_data = random_data();
                        }
                        if (write_data != NULL)
                        {
                            write_to_file(task_pid, write_data, lock);
                            free(write_data);
                        }

                        release_lock_info(lock);

                        /* Check active processes and waiting tasks BEFORE removing ourselves */
                        char active_list_before_remove[2048];
                        get_active_processes_list(active_list_before_remove, sizeof(active_list_before_remove));
                        int waiting_before_remove = count_queue_tasks();

                        /* Remove from active processes AFTER executing */
                        remove_active_process(getpid());

                        /* Log that we removed ourselves from active processes */
                        snprintf(log_message, sizeof(log_message),
                                 "Worker: Removed WRITE process PID %d from active processes. Before removal: active=%s, waiting=%d\n",
                                 (int)getpid(), active_list_before_remove, waiting_before_remove);
                        stream_write_logfile(log_message);

                        /* Increment completed tasks counter */
                        increment_completed_tasks();

                        snprintf(log_message, sizeof(log_message),
                                 "Worker: Successfully processed write task for PID %d. Active processes at start: %s\n",
                                 (int)task_pid, active_list_check);
                        stream_write_logfile(log_message);

                        /* Print statistics after completing task */
                        print_statistics();
                    }
                    else
                    {
                        /* Lock not available, put back to queue */
                        /* Re-check active processes after lock failure */
                        get_active_processes_list(active_list_check, sizeof(active_list_check));
                        add_task_to_queue_file(task_pid, lock_type, task_data);

                        snprintf(log_message, sizeof(log_message),
                                 "Worker: WRITE task for PID %d rejected - failed to acquire exclusive lock, put back to queue. Blocking processes: %s\n",
                                 (int)task_pid, active_list_check);
                        stream_write_logfile(log_message);
                    }
                }
                else
                {
                    /* Active processes exist, put back to queue */
                    add_task_to_queue_file(task_pid, lock_type, task_data);

                    snprintf(log_message, sizeof(log_message),
                             "Worker: WRITE task for PID %d rejected - active processes detected (read or write), put back to queue. Blocking processes: %s\n",
                             (int)task_pid, active_list_check);
                    stream_write_logfile(log_message);
                }
            }
        }
        else
        {
            /* Queue is empty - log occasionally */
            static int empty_count = 0;
            empty_count++;
            if (empty_count % 10 == 0)  /* Log every 10th check */
            {
                snprintf(log_message, sizeof(log_message),
                         "Worker: Queue is empty, waiting for tasks...\n");
                stream_write_logfile(log_message);
            }
        }

        /* Sleep before next attempt - but only if we didn't process a task */
        /* If we processed a task, check immediately for next task to catch active processes */
        if (result != 0)
        {
            sleep(WORKER_SLEEP);
        }
        else
        {
            /* Just processed a task, add delay to allow other processes to register and execute */
            /* This creates opportunity for READ tasks to see active WRITE processes */
            usleep(200000);  /* 200ms delay to allow processes to register and execute */
        }
    }
}

/* Task Manager process - accepts tasks and adds to queue */
void task_manager_process(int argc, char* argv[])
{
    pid_t pid = getpid();
    char* write_data = NULL;

    /* Initialize random seed */
    srand((unsigned int)(time(NULL) ^ pid));

    if (argc < 2) {
        fprintf(stderr, "Usage: %s <read|write> [data]\n", argv[0]);
        fprintf(stderr, "  read  - add read task to queue\n");
        fprintf(stderr, "  write - add write task to queue\n");
        exit(1);
    }

    /* Process the task */
    if (strcmp(argv[1], "read") == 0)
    {
        add_task_to_queue_file(pid, SHARED_LOCK, NULL);
    }
    else if (strcmp(argv[1], "write") == 0)
    {
        /* Always generate random data */
        write_data = random_data();

        if (write_data == NULL)
        {
            fprintf(stderr, "Fatal error: Failed to generate random data\n");
            exit(1);
        }

        add_task_to_queue_file(pid, EXCLUSIVE_LOCK, write_data);
        free(write_data);
    }
    else
    {
        fprintf(stderr, "Parameter error: Unknown command: %s\n", argv[1]);
        exit(1);
    }

    printf("Task Manager: Task added to queue successfully\n");
}

int main(int argc, char* argv[])
{
    if (argc >= 2 && strcmp(argv[1], "worker") == 0)
    {
        /* Worker process mode */
        worker_process();
        return 0;
    }

    /* Task Manager process mode - accepts tasks from bash */
    task_manager_process(argc, argv);
    return 0;
}
