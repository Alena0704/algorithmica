/* Reader-Writer Lock System with Queue Management */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/wait.h>
#include <sys/mman.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <signal.h>
#include <limits.h>
#include <semaphore.h>

#define FILENAME "edit_parallel"
#define SHM_ACTIVE_FILE "active_processes_shm.dat"
#define SEM_ACTIVE_NAME "/active_processes_sem"
#define COMPLETED_TASKS_FILE "completed_tasks.dat"
#define PLANNED_TASKS_FILE "planned_tasks.dat"
#define LOGFILE "logfile.txt"
#define SHM_QUEUE_NAME "/task_queue_shm"
#define SHM_QUEUE_FILE "task_queue_shm.dat"
#define SEM_QUEUE_NAME "/task_queue_sem"
#define HASH_TABLE_SIZE 256
#define MAX_QUEUE_SIZE 1024
#define MAX_ACTIVE_PROCESSES 512

#define WORKER_SLEEP 1
#define MIN_DELAY_MS 500
#define MAX_DELAY_MS 2000
#define MAX_DATA_SIZE 1024
#define BUFFER_SIZE 2048
#define LOG_BUFFER_SIZE 4096
#define ACTIVE_LIST_SIZE 2048

#define CLEANUP_INTERVAL 20
#define STATS_INTERVAL 5
#define EMPTY_QUEUE_LOG_INTERVAL 10
#define MAX_RECHECKS 5
#define RECHECK_DELAY_US 100000
#define FORK_DELAY_US 50000
#define POST_TASK_DELAY_US 200000


typedef enum {
    SHARED_LOCK = 0,
    EXCLUSIVE_LOCK = 1,
    INVALID_LOCK = 2
} LockType;

typedef struct {
    int fd;
    LockType lock_type;
} LockInfo;

typedef struct TaskData {
    pid_t pid;
    LockType lock_type;
    char data[MAX_DATA_SIZE];
    int next_index;
    int in_use;
} TaskData;

typedef struct PidNode {
    pid_t pid;
    int next_index;
    int in_use;
    int was_in_queue;
} PidNode;

typedef struct {
    int bucket_heads[HASH_TABLE_SIZE];
    TaskData tasks[HASH_TABLE_SIZE * 4];
    int free_list_head;
    int count;
} TaskDataHashTable;

typedef struct {
    int head_index;
    int tail_index;
    PidNode pids[MAX_QUEUE_SIZE];
    int free_list_head;
    int count;
    int initialized;
} PidSortedList;

typedef struct {
    TaskDataHashTable hash_table;
    PidSortedList sorted_list;
    int initialized;
} TaskQueue;

typedef struct ActiveProcess {
    pid_t pid;
    LockType lock_type;
    int next_index;
    int in_use;
} ActiveProcess;

typedef struct {
    int bucket_heads[HASH_TABLE_SIZE];
    ActiveProcess processes[HASH_TABLE_SIZE * 4];
    int free_list_head;
    int count;
    int initialized;
} ActiveProcessList;

static ActiveProcessList* g_active_list = NULL;
static sem_t* g_active_sem = NULL;
static size_t g_active_list_size = 0;

#define Assert(condition) \
do { \
    if(!(condition)) { \
        fprintf(stderr, "Fatal error: Assertion failed: %s\n", #condition); \
        exit(1); \
    } \
} while (0)

// Lock management

LockInfo* try_acquire_lock_nonblocking(LockType lock_type)
{
    LockInfo* lock_info;

    lock_info = malloc(sizeof(LockInfo));
    if(lock_info == NULL || lock_type == INVALID_LOCK)
    {
        fprintf(stderr, "Fatal error: Failed to allocate lock info\n");
        exit(1);
    }

    lock_info->lock_type = lock_type;

    lock_info->fd = open(FILENAME,
                            lock_type == SHARED_LOCK ? O_RDONLY | O_CREAT :
                                                        O_RDWR | O_CREAT | O_TRUNC,
                            0644);
    if(lock_info->fd < 0)
    {
        free(lock_info);
        return NULL;
    }

    if(lock_type == SHARED_LOCK)
    {
        if(flock(lock_info->fd, LOCK_SH | LOCK_NB) < 0)
        {
            close(lock_info->fd);
            free(lock_info);
            return NULL;
        }
    }
    else
    {
        if(flock(lock_info->fd, LOCK_EX | LOCK_NB) < 0)
        {
            close(lock_info->fd);
            free(lock_info);
            return NULL;
        }
    }

    return lock_info;
}

void release_lock_info(LockInfo* lock_info)
{
    Assert(lock_info != NULL);

    if(flock(lock_info->fd, LOCK_UN) < 0)
    {
        fprintf(stderr, "Fatal error: Failed to release %s lock\n",
                            lock_info->lock_type == SHARED_LOCK ? "SHARED" :
                                "EXCLUSIVE");
        exit(1);
    }

    Assert(lock_info->fd >= 0);

    if(close(lock_info->fd) != 0)
    {
        fprintf(stderr, "Fatal error: Failed to close file\n");
        exit(1);
    }

    free(lock_info);
}

void stream_write_logfile(const char* message)
{
    if(message == NULL)
        return;

    int logfile = open(LOGFILE, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if(logfile >= 0)
    {
        size_t msg_len = strlen(message);
        if(msg_len > 0)
            write(logfile, message, msg_len);
        close(logfile);
    }
}

void random_delay_ms(int min_ms, int max_ms)
{
    int delay_ms;
    struct timespec ts;

    if(max_ms <= min_ms)
        delay_ms = min_ms;
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
    char read_data[BUFFER_SIZE] = {0};
    ssize_t bytes_read;
    char log_message[LOG_BUFFER_SIZE];
    int total_read = 0;

    if(lock == NULL || lock->fd < 0)
    {
        fprintf(stderr, "Fatal error: Invalid lock in read_from_file\n");
        return;
    }

    random_delay_ms(MIN_DELAY_MS, MAX_DELAY_MS);
    lseek(lock->fd, 0, SEEK_SET);

    while ((bytes_read = read(lock->fd, buffer, sizeof(buffer) - 1)) > 0)
    {
        buffer[bytes_read] = '\0';
        if(total_read + (int)bytes_read < (int)(sizeof(read_data) - 1))
        {
            strcat(read_data, buffer);
            total_read += (int)bytes_read;
        }
    }

    if(total_read > 0)
    {
        snprintf(log_message, sizeof(log_message),
                 "Process %d: Read %d bytes: %s\n", (int)pid, total_read, read_data);
        stream_write_logfile(log_message);
    }
    else
    {
        snprintf(log_message, sizeof(log_message),
                 "Process %d: File is empty\n", (int)pid);
        stream_write_logfile(log_message);
    }
}

char* random_data(void)
{
    char* data;
    int len = 100;

    data = (char*)malloc(len + 1);
    if(data == NULL)
    {
        fprintf(stderr, "Fatal error: Failed to allocate memory for random data\n");
        exit(1);
    }

    for (int i = 0; i < len; i++)
        data[i] = 'a' + rand() % 26;
    data[len] = '\0';
    return data;
}

void write_to_file(pid_t pid, char* write_data, LockInfo* lock)
{
    ssize_t bytes_written;
    char log_message[LOG_BUFFER_SIZE];
    char write_buffer[BUFFER_SIZE];

    if(lock == NULL || lock->fd < 0)
    {
        fprintf(stderr, "Fatal error: Invalid lock in write_to_file\n");
        return;
    }

    if(write_data == NULL)
    {
        fprintf(stderr, "Fatal error: write_data is NULL\n");
        return;
    }

    random_delay_ms(MIN_DELAY_MS, MAX_DELAY_MS);
    snprintf(write_buffer, sizeof(write_buffer), "Process %d: %s\n", (int)pid, write_data);

    bytes_written = write(lock->fd, write_buffer, strlen(write_buffer));
    if(bytes_written < 0)
    {
        fprintf(stderr, "Fatal error: write failed\n");
        return;
    }

    fsync(lock->fd);
    printf("Process %d: Wrote %zd bytes: %s", (int)pid, bytes_written, write_buffer);
    stream_write_logfile(log_message);
}

// Active process management
static int init_active_processes_list(void);
static void cleanup_active_processes_list(void);
static int active_list_add_process(pid_t pid, LockType lock_type);
static int active_list_remove_process(pid_t pid);
static int active_list_has_process_by_type(LockType filter_type);
static int active_list_count_processes_by_type(LockType filter_type);
static void active_list_get_formatted_list(char* buffer, size_t buffer_size);
static void active_list_cleanup_dead_processes(void);

static int has_active_process_by_type(LockType filter_type)
{
    if((g_active_list == NULL || g_active_sem == NULL) && init_active_processes_list() < 0)
        return 0;

    sem_wait(g_active_sem);
    int result = active_list_has_process_by_type(filter_type);
    sem_post(g_active_sem);
    return result;
}

int has_active_write_process(void)
{
    return has_active_process_by_type(EXCLUSIVE_LOCK);
}

int has_any_active_process(void)
{
    return has_active_process_by_type(INVALID_LOCK);
}

int add_active_process(pid_t pid, LockType lock_type)
{
    if(g_active_list == NULL || g_active_sem == NULL)
    {
        if(init_active_processes_list() < 0)
            return -1;
    }

    sem_wait(g_active_sem);
    int result = active_list_add_process(pid, lock_type);
    sem_post(g_active_sem);
    return result;
}

int remove_active_process(pid_t pid)
{
    if((g_active_list == NULL || g_active_sem == NULL) && init_active_processes_list() < 0)
        return -1;

    sem_wait(g_active_sem);
    int result = active_list_remove_process(pid);
    sem_post(g_active_sem);
    return result;
}

void cleanup_active_processes(void)
{
    if((g_active_list == NULL || g_active_sem == NULL) && init_active_processes_list() < 0)
        return;

    sem_wait(g_active_sem);
    active_list_cleanup_dead_processes();
    sem_post(g_active_sem);
}

// Queue management (shared memory)
static TaskQueue* g_queue = NULL;
static sem_t* g_queue_sem = NULL;
static size_t g_queue_size = 0;

static int hash_pid(pid_t pid)
{
    return ((int)pid) % HASH_TABLE_SIZE;
}


static void init_hash_table_buckets(int* bucket_heads, int size)
{
    for (int i = 0; i < size; i++)
    {
        bucket_heads[i] = -1;
    }
}

static void init_task_data_free_list(TaskData* tasks, int array_size, int* free_list_head)
{
    *free_list_head = -1;
    for (int i = 0; i < array_size - 1; i++)
    {
        tasks[i].next_index = i + 1;
        tasks[i].in_use = 0;
    }
    tasks[array_size - 1].next_index = -1;
    *free_list_head = 0;
}
static void init_active_process_free_list(ActiveProcess* processes, int array_size, int* free_list_head)
{
    *free_list_head = -1;

    for (int i = 0; i < array_size - 1; i++)
    {
        processes[i].next_index = i + 1;
        processes[i].in_use = 0;
    }
    processes[array_size - 1].next_index = -1;
    *free_list_head = 0;
}

/* Open or create shared memory file (file-based for macOS compatibility) */
static int open_shared_memory_file(const char* filename, size_t shm_size, int* is_new)
{
    int shm_fd;
    struct stat st;

    *is_new = 0;
    shm_fd = open(filename, O_RDWR, 0666);
    if(shm_fd < 0)
    {
        /* Doesn't exist, create new */
        shm_fd = open(filename, O_CREAT | O_RDWR, 0666);
        if(shm_fd < 0)
            return -1;
        *is_new = 1;
    }
    else
    {
        /* Check if size matches (might need to reinit if size changed) */
        if (fstat(shm_fd, &st) != 0 || st.st_size == 0 || st.st_size != (off_t)shm_size)
            *is_new = 1;  /* Can't stat or size mismatch, assume new */
    }

    /* Set size (always call ftruncate for file-based approach) */
    if(ftruncate(shm_fd, shm_size) < 0)
    {
        close(shm_fd);
        return -1;
    }

    return shm_fd;
}

/* Initialize shared memory queue (hash table + sorted list) */
static int init_queue_sorted_list(void)
{
    int shm_fd;
    size_t shm_size = sizeof(TaskQueue);
    int is_new = 0;

    /* Open or create shared memory file */
    shm_fd = open_shared_memory_file(SHM_QUEUE_FILE, shm_size, &is_new);
    if(shm_fd < 0)
    {
        perror("Fatal error: Failed to create shared memory file");
        return -1;
    }

    /* Create or open semaphore FIRST (before mapping, to protect initialization) */
    g_queue_sem = sem_open(SEM_QUEUE_NAME, O_CREAT, 0666, 1);
    if(g_queue_sem == SEM_FAILED)
    {
        perror("Fatal error: Failed to create semaphore");
        close(shm_fd);
        return -1;
    }

    /* Map shared memory */
    g_queue = (TaskQueue*)mmap(NULL, shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if(g_queue == MAP_FAILED)
    {
        perror("Fatal error: Failed to map shared memory");
        sem_close(g_queue_sem);
        close(shm_fd);
        return -1;
    }
    g_queue_size = shm_size;

    /* Initialize if first time (or if not initialized yet) - protected by semaphore */
    sem_wait(g_queue_sem);

    if(is_new || !g_queue->initialized)
    {
        memset(g_queue, 0, shm_size);

        /* Initialize hash table */
        init_hash_table_buckets(g_queue->hash_table.bucket_heads, HASH_TABLE_SIZE);
        init_task_data_free_list(g_queue->hash_table.tasks, HASH_TABLE_SIZE * 4,
                                 &g_queue->hash_table.free_list_head);
        g_queue->hash_table.count = 0;

        /* Initialize sorted list */
        g_queue->sorted_list.head_index = -1;
        g_queue->sorted_list.tail_index = -1;
        g_queue->sorted_list.free_list_head = -1;
        g_queue->sorted_list.count = 0;
        g_queue->sorted_list.initialized = 1;

        /* Initialize free list for sorted list */
        for (int i = 0; i < MAX_QUEUE_SIZE - 1; i++)
        {
            g_queue->sorted_list.pids[i].next_index = i + 1;
            g_queue->sorted_list.pids[i].in_use = 0;
            g_queue->sorted_list.pids[i].was_in_queue = 0;
        }
        g_queue->sorted_list.pids[MAX_QUEUE_SIZE - 1].next_index = -1;
        g_queue->sorted_list.free_list_head = 0;

        g_queue->initialized = 1;
    }

    sem_post(g_queue_sem);

    close(shm_fd);
    return 0;
}

/* Cleanup shared memory (called on exit) */
static void cleanup_queue_sorted_list(void)
{
    if(g_queue != NULL && g_queue != MAP_FAILED && g_queue_size > 0)
    {
        munmap(g_queue, g_queue_size);
        g_queue = NULL;
        g_queue_size = 0;
    }
    if(g_queue_sem != NULL && g_queue_sem != SEM_FAILED)
    {
        sem_close(g_queue_sem);
        g_queue_sem = NULL;
    }
    /* Don't unlink the file - other processes may still be using it */
}

/* Add task data to hash table (or update if exists) */
static int hash_table_add_task_data(pid_t pid, LockType lock_type, const char* data)
{
    if(g_queue == NULL)
        return -1;

    int bucket = hash_pid(pid);
    int current = g_queue->hash_table.bucket_heads[bucket];

    /* Check if task data already exists (update if it does) */
    while (current >= 0 && current < HASH_TABLE_SIZE * 4)
    {
        if(g_queue->hash_table.tasks[current].in_use && g_queue->hash_table.tasks[current].pid == pid)
        {
            /* Update existing task data */
            g_queue->hash_table.tasks[current].lock_type = lock_type;
            if(data != NULL)
            {
                strncpy(g_queue->hash_table.tasks[current].data, data, MAX_DATA_SIZE - 1);
                g_queue->hash_table.tasks[current].data[MAX_DATA_SIZE - 1] = '\0';
            }
            else
            {
                g_queue->hash_table.tasks[current].data[0] = '\0';
            }
            return 0;
        }
        if(g_queue->hash_table.tasks[current].next_index < 0)
            break;
        current = g_queue->hash_table.tasks[current].next_index;
    }

    /* Find free slot */
    if(g_queue->hash_table.free_list_head < 0)
        return -1;

    int free_index = g_queue->hash_table.free_list_head;
    g_queue->hash_table.free_list_head = g_queue->hash_table.tasks[free_index].next_index;

    /* Initialize new task data */
    g_queue->hash_table.tasks[free_index].pid = pid;
    g_queue->hash_table.tasks[free_index].lock_type = lock_type;
    if(data != NULL)
    {
        strncpy(g_queue->hash_table.tasks[free_index].data, data, MAX_DATA_SIZE - 1);
        g_queue->hash_table.tasks[free_index].data[MAX_DATA_SIZE - 1] = '\0';
    }
    else
    {
        g_queue->hash_table.tasks[free_index].data[0] = '\0';
    }
    g_queue->hash_table.tasks[free_index].in_use = 1;
    g_queue->hash_table.tasks[free_index].next_index = -1;

    /* Add to bucket chain */
    if(g_queue->hash_table.bucket_heads[bucket] >= 0)
    {
        /* Find end of chain (walk to the end) */
        int chain_end = g_queue->hash_table.bucket_heads[bucket];
        while (g_queue->hash_table.tasks[chain_end].next_index >= 0)
        {
            chain_end = g_queue->hash_table.tasks[chain_end].next_index;
        }
        g_queue->hash_table.tasks[chain_end].next_index = free_index;
    }
    else
    {
        g_queue->hash_table.bucket_heads[bucket] = free_index;
    }

    g_queue->hash_table.count++;
    return 0;
}

/* Add PID to sorted list */
static int sorted_list_add_pid(pid_t pid, int was_in_queue)
{
    if(g_queue == NULL)
        return -1;

    /* Check if PID already exists in list */
    int existing_index = -1;
    int current = g_queue->sorted_list.head_index;
    while (current >= 0 && current < MAX_QUEUE_SIZE)
    {
        if(g_queue->sorted_list.pids[current].in_use && g_queue->sorted_list.pids[current].pid == pid)
        {
            existing_index = current;
            break;
        }
        current = g_queue->sorted_list.pids[current].next_index;
    }

    if(existing_index >= 0)
    {
        /* PID already exists - was rejected, need to reinsert in sorted position */
        /* First, find where to insert (before removing - otherwise we'll lose the position) */
        int insert_pos = -1;
        int insert_prev = -1;
        int search_current = g_queue->sorted_list.head_index;
        while (search_current >= 0 && search_current < MAX_QUEUE_SIZE)
        {
            /* Stop when we find a PID greater than current */
            if(g_queue->sorted_list.pids[search_current].in_use && search_current != existing_index && (int)g_queue->sorted_list.pids[search_current].pid > (int)pid)
            {
                insert_pos = search_current;
                break;
            }
            if(search_current != existing_index)
                insert_prev = search_current;
            search_current = g_queue->sorted_list.pids[search_current].next_index;
        }

        /* Remove from current position */
        int prev_index = -1;
        current = g_queue->sorted_list.head_index;
        while (current >= 0 && current != existing_index)
        {
            prev_index = current;
            current = g_queue->sorted_list.pids[current].next_index;
        }

        if(prev_index >= 0)
        {
            g_queue->sorted_list.pids[prev_index].next_index = g_queue->sorted_list.pids[existing_index].next_index;
        }
        else
        {
            g_queue->sorted_list.head_index = g_queue->sorted_list.pids[existing_index].next_index;
        }

        if(g_queue->sorted_list.tail_index == existing_index)
            g_queue->sorted_list.tail_index = prev_index;

        g_queue->sorted_list.pids[existing_index].was_in_queue = 1;

        /* Insert at found position */
        if(insert_pos >= 0)
        {
            /* Insert before insert_pos */
            g_queue->sorted_list.pids[existing_index].next_index = insert_pos;
            if(insert_prev >= 0)
                g_queue->sorted_list.pids[insert_prev].next_index = existing_index;
            else
                /* Inserting at head (smallest PID) */
                g_queue->sorted_list.head_index = existing_index;
        }
        else
        {
            /* Insert at end (largest PID or empty list) */
            if(g_queue->sorted_list.tail_index >= 0)
                g_queue->sorted_list.pids[g_queue->sorted_list.tail_index].next_index = existing_index;
            g_queue->sorted_list.tail_index = existing_index;
            g_queue->sorted_list.pids[existing_index].next_index = -1;
            if(g_queue->sorted_list.head_index < 0)
                g_queue->sorted_list.head_index = existing_index;  /* Was empty list */
        }

        return 0;
    }

    /* New PID - add to end of list if was_in_queue=0, otherwise insert in sorted position */
    if(g_queue->sorted_list.free_list_head < 0)
        return -1;  /* No free slots */

    int free_index = g_queue->sorted_list.free_list_head;
    g_queue->sorted_list.free_list_head = g_queue->sorted_list.pids[free_index].next_index;

    /* Initialize new PID node */
    g_queue->sorted_list.pids[free_index].pid = pid;
    g_queue->sorted_list.pids[free_index].in_use = 1;
    g_queue->sorted_list.pids[free_index].next_index = -1;
    g_queue->sorted_list.pids[free_index].was_in_queue = was_in_queue;

    if(was_in_queue == 0)
    {
        /* New task - add to end of list (as per requirement) */
        if(g_queue->sorted_list.tail_index >= 0)
            g_queue->sorted_list.pids[g_queue->sorted_list.tail_index].next_index = free_index;
        g_queue->sorted_list.tail_index = free_index;
        if(g_queue->sorted_list.head_index < 0)
            g_queue->sorted_list.head_index = free_index;
    }
    else
    {
        /* Repeated task - insert in sorted position (insertion sort from beginning) */
        int insert_pos = -1;
        int insert_prev = -1;
        int search_current = g_queue->sorted_list.head_index;
        while (search_current >= 0 && search_current < MAX_QUEUE_SIZE)
        {
            /* Stop when we find a PID greater than current */
            if(g_queue->sorted_list.pids[search_current].in_use && (int)g_queue->sorted_list.pids[search_current].pid > (int)pid)
            {
                insert_pos = search_current;
                break;
            }
            insert_prev = search_current;
            search_current = g_queue->sorted_list.pids[search_current].next_index;
        }

        /* Insert at found position */
        if(insert_pos >= 0)
        {
            /* Insert before insert_pos */
            g_queue->sorted_list.pids[free_index].next_index = insert_pos;
            if(insert_prev >= 0)
                g_queue->sorted_list.pids[insert_prev].next_index = free_index;
            else
                /* Inserting at head */
                g_queue->sorted_list.head_index = free_index;
        }
        else
        {
            /* Insert at end */
            if(g_queue->sorted_list.tail_index >= 0)
                g_queue->sorted_list.pids[g_queue->sorted_list.tail_index].next_index = free_index;
            g_queue->sorted_list.tail_index = free_index;
            if(g_queue->sorted_list.head_index < 0)
                g_queue->sorted_list.head_index = free_index;
        }
    }

    g_queue->sorted_list.count++;
    return 0;
}

/* Add task to queue (hash table + sorted list) */
static int sorted_list_add_task(pid_t pid, LockType lock_type, const char* data)
{
    if((g_queue == NULL || g_queue_sem == NULL) && init_queue_sorted_list() < 0)
        return -1;

    sem_wait(g_queue_sem);

    /* Check if PID already exists in sorted list to determine if it's a new task */
    int was_in_queue = 0;
    int current = g_queue->sorted_list.head_index;
    while (current >= 0 && current < MAX_QUEUE_SIZE)
    {
        if(g_queue->sorted_list.pids[current].in_use && g_queue->sorted_list.pids[current].pid == pid)
        {
            was_in_queue = 1;
            break;
        }
        current = g_queue->sorted_list.pids[current].next_index;
    }

    /* Add task data to hash table */
    if(hash_table_add_task_data(pid, lock_type, data) < 0)
    {
        sem_post(g_queue_sem);
        return -1;
    }

    /* Add PID to sorted list */
    if(sorted_list_add_pid(pid, was_in_queue) < 0)
    {
        sem_post(g_queue_sem);
        return -1;
    }

    sem_post(g_queue_sem);
    return 0;
}

/* Remove task data from hash table */
static void hash_table_remove_task_data(pid_t pid)
{
    if(g_queue == NULL)
        return;

    int bucket = hash_pid(pid);
    int prev_index = -1;
    int current = g_queue->hash_table.bucket_heads[bucket];

    /* Find task in hash table */
    while (current >= 0 && current < HASH_TABLE_SIZE * 4)
    {
        if(g_queue->hash_table.tasks[current].in_use && g_queue->hash_table.tasks[current].pid == pid)
        {
            /* Remove from chain */
            if(prev_index >= 0)
            {
                g_queue->hash_table.tasks[prev_index].next_index = g_queue->hash_table.tasks[current].next_index;
            }
            else if(g_queue->hash_table.bucket_heads[bucket] == current)
            {
                g_queue->hash_table.bucket_heads[bucket] = g_queue->hash_table.tasks[current].next_index;
            }

            /* Add to free list */
            g_queue->hash_table.tasks[current].in_use = 0;
            g_queue->hash_table.tasks[current].next_index = g_queue->hash_table.free_list_head;
            g_queue->hash_table.free_list_head = current;

            g_queue->hash_table.count--;
            return;
        }
        prev_index = current;
        current = g_queue->hash_table.tasks[current].next_index;
    }
}

/* Get task data from hash table by PID (lookup) */
static int hash_table_get_task_data(pid_t pid, LockType* lock_type, char* data, size_t data_size)
{
    if(g_queue == NULL)
        return -1;

    int bucket = hash_pid(pid);
    int current = g_queue->hash_table.bucket_heads[bucket];

    /* Find task in hash table */
    while (current >= 0 && current < HASH_TABLE_SIZE * 4)
    {
        if(g_queue->hash_table.tasks[current].in_use && g_queue->hash_table.tasks[current].pid == pid)
        {
            *lock_type = g_queue->hash_table.tasks[current].lock_type;
            if(data != NULL && data_size > 0)
            {
                strncpy(data, g_queue->hash_table.tasks[current].data, data_size - 1);
                data[data_size - 1] = '\0';
            }
            return 0;
        }
        current = g_queue->hash_table.tasks[current].next_index;
    }

    return -1;
}

/* Get and remove first task from sorted list (minimum PID - FIFO with priority) */
static int sorted_list_get_task(pid_t* pid, LockType* lock_type, char* data, size_t data_size)
{
    if(g_queue == NULL || g_queue_sem == NULL)
    {
        if(init_queue_sorted_list() < 0)
            return -1;
    }

    sem_wait(g_queue_sem);

    if(g_queue->sorted_list.count == 0 || g_queue->sorted_list.head_index < 0)
    {
        sem_post(g_queue_sem);
        return -1;
    }

    /* Get first PID from sorted list */
    int selected_index = g_queue->sorted_list.head_index;
    pid_t selected_pid = g_queue->sorted_list.pids[selected_index].pid;

    /* Get task data from hash table */
    if(hash_table_get_task_data(selected_pid, lock_type, data, data_size) < 0)
    {
        sem_post(g_queue_sem);
        return -1;
    }

    /* Copy PID */
    *pid = selected_pid;

    /* Remove PID from sorted list */
    g_queue->sorted_list.head_index = g_queue->sorted_list.pids[selected_index].next_index;
    if(g_queue->sorted_list.tail_index == selected_index)
        g_queue->sorted_list.tail_index = -1;

    /* Add to free list */
    g_queue->sorted_list.pids[selected_index].in_use = 0;
    g_queue->sorted_list.pids[selected_index].was_in_queue = 0;
    g_queue->sorted_list.pids[selected_index].next_index = g_queue->sorted_list.free_list_head;
    g_queue->sorted_list.free_list_head = selected_index;

    g_queue->sorted_list.count--;

    /* Remove task data from hash table */
    hash_table_remove_task_data(selected_pid);

    sem_post(g_queue_sem);
    return 0;
}

/* Count tasks in sorted list (thread-safe) */
static int sorted_list_count_tasks(void)
{
    if(g_queue == NULL || g_queue_sem == NULL)
    {
        if(init_queue_sorted_list() < 0)
            return 0;
    }

    sem_wait(g_queue_sem);
    int count = g_queue->sorted_list.count;
    sem_post(g_queue_sem);
    return count;
}

// Active processes management

static int init_active_processes_list(void)
{
    int shm_fd;
    size_t shm_size = sizeof(ActiveProcessList);
    int is_new = 0;

    /* Open or create shared memory file */
    shm_fd = open_shared_memory_file(SHM_ACTIVE_FILE, shm_size, &is_new);
    if(shm_fd < 0)
    {
        perror("Fatal error: Failed to create shared memory file for active processes");
        return -1;
    }

    /* Try to unlink existing semaphore first (in case it's in bad state) */
    sem_unlink(SEM_ACTIVE_NAME);

    /* Create or open semaphore FIRST (before mapping, to protect initialization) */
    g_active_sem = sem_open(SEM_ACTIVE_NAME, O_CREAT | O_EXCL, 0666, 1);
    if(g_active_sem == SEM_FAILED && errno == EEXIST)
    {
        /* Semaphore already exists, try to open it */
        g_active_sem = sem_open(SEM_ACTIVE_NAME, 0);
    }

    if(g_active_sem == SEM_FAILED)
    {
        perror("Fatal error: Failed to create semaphore for active processes");
        close(shm_fd);
        return -1;
    }

    /* Map shared memory */
    g_active_list = (ActiveProcessList*)mmap(NULL, shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if(g_active_list == MAP_FAILED)
    {
        perror("Fatal error: Failed to map shared memory for active processes");
        sem_close(g_active_sem);
        close(shm_fd);
        return -1;
    }
    g_active_list_size = shm_size;

    /* Initialize if first time (or if not initialized yet) - protected by semaphore */
    /* Don't log before sem_wait - it may block if many processes write simultaneously */
    int sem_result = sem_wait(g_active_sem);
    if(sem_result != 0)
    {
        perror("sem_wait failed");
        return -1;
    }

    if(g_active_list == NULL || g_active_list == MAP_FAILED)
    {
        sem_post(g_active_sem);
        return -1;
    }

    if(is_new || !g_active_list->initialized)
    {

        memset(g_active_list, 0, shm_size);

        /* Initialize hash table */
        init_hash_table_buckets(g_active_list->bucket_heads, HASH_TABLE_SIZE);
        init_active_process_free_list(g_active_list->processes, HASH_TABLE_SIZE * 4,
                                     &g_active_list->free_list_head);
        g_active_list->count = 0;
        g_active_list->initialized = 1;
    }

    sem_post(g_active_sem);

    close(shm_fd);
    return 0;
}

/* Cleanup shared memory for active processes (called on exit) */
static void cleanup_active_processes_list(void)
{
    if(g_active_list != NULL && g_active_list != MAP_FAILED && g_active_list_size > 0)
    {
        munmap(g_active_list, g_active_list_size);
        g_active_list = NULL;
        g_active_list_size = 0;
    }
    if(g_active_sem != NULL && g_active_sem != SEM_FAILED)
    {
        sem_close(g_active_sem);
        g_active_sem = NULL;
    }
}

/* Add active process to hash table (or update if exists) */
static int active_list_add_process(pid_t pid, LockType lock_type)
{
    if(g_active_list == NULL)
        return -1;

    int bucket = hash_pid(pid);
    int current = g_active_list->bucket_heads[bucket];

    /* Check if process already exists */
    while (current >= 0 && current < HASH_TABLE_SIZE * 4)
    {
        if(g_active_list->processes[current].in_use &&
            g_active_list->processes[current].pid == pid)
        {
            /* Update existing process */
            g_active_list->processes[current].lock_type = lock_type;
            return 0;
        }
        if(g_active_list->processes[current].next_index < 0)
            break;
        current = g_active_list->processes[current].next_index;
    }

    /* Find free slot */
    if(g_active_list->free_list_head < 0)
        return -1;  /* No free slots */

    int free_index = g_active_list->free_list_head;
    g_active_list->free_list_head =
                g_active_list->processes[free_index].next_index;

    /* Initialize new process */
    g_active_list->processes[free_index].pid = pid;
    g_active_list->processes[free_index].lock_type = lock_type;
    g_active_list->processes[free_index].in_use = 1;
    g_active_list->processes[free_index].next_index = -1;

    /* Add to bucket chain */
    if(g_active_list->bucket_heads[bucket] >= 0)
    {
        /* Find end of chain */
        int chain_end = g_active_list->bucket_heads[bucket];

        while (g_active_list->processes[chain_end].next_index >= 0)
            chain_end = g_active_list->processes[chain_end].next_index;

        g_active_list->processes[chain_end].next_index = free_index;
    }
    else
        /* First element in bucket */
        g_active_list->bucket_heads[bucket] = free_index;

    g_active_list->count++;
    return 0;
}

/* Remove active process from hash table (cleanup) */
static int active_list_remove_process(pid_t pid)
{
    if(g_active_list == NULL)
        return -1;

    int bucket = hash_pid(pid);
    int prev_index = -1;
    int current = g_active_list->bucket_heads[bucket];

    /* Find process in hash table */
    while (current >= 0 && current < HASH_TABLE_SIZE * 4)
    {
        if(g_active_list->processes[current].in_use && g_active_list->processes[current].pid == pid)
        {
            /* Remove from chain */
            if(prev_index >= 0)
                g_active_list->processes[prev_index].next_index = g_active_list->processes[current].next_index;
            else if(g_active_list->bucket_heads[bucket] == current)
                g_active_list->bucket_heads[bucket] = g_active_list->processes[current].next_index;

            /* Add to free list */
            g_active_list->processes[current].in_use = 0;
            g_active_list->processes[current].next_index = g_active_list->free_list_head;
            g_active_list->free_list_head = current;

            g_active_list->count--;
            return 0;
        }
        prev_index = current;
        current = g_active_list->processes[current].next_index;
    }

    return -1;  /* Process not found */
}

/* Helper: Check if process is alive, remove if dead (cleanup zombie entries) */
static int check_and_cleanup_process(pid_t pid)
{
    if(kill(pid, 0) == 0)
        return 1;  /* Process is alive */
    /* Process is dead, remove it (cleanup) */
    active_list_remove_process(pid);
    return 0;  /* Process was dead */
}

/* Helper: Iterate through all active processes (callback-based for flexibility) */
typedef int (*ProcessCallback)(pid_t pid, LockType lock_type, void* user_data);

static void iterate_active_processes(ProcessCallback callback, void* user_data, LockType filter_type)
{
    if(g_active_list == NULL)
        return;

    for (int i = 0; i < HASH_TABLE_SIZE; i++)
    {
        int current = g_active_list->bucket_heads[i];
        while (current >= 0 && current < HASH_TABLE_SIZE * 4)
        {
            if(g_active_list->processes[current].in_use)
            {
                pid_t pid = g_active_list->processes[current].pid;
                LockType lock_type = g_active_list->processes[current].lock_type;

                /* Check if process is still alive (cleanup dead ones) */
                if(check_and_cleanup_process(pid))
                {
                    /* If filter_type is INVALID_LOCK, match any; otherwise match specific type */
                    if((filter_type == INVALID_LOCK || lock_type == filter_type) &&
                                            callback(pid, lock_type, user_data) != 0)
                            return;  /* Callback requested early termination (found what we needed) */
                }
            }
            current = g_active_list->processes[current].next_index;
        }
    }
}

/* Callback for checking existence (stop as soon as we find one) */
static int check_exists_callback(pid_t pid, LockType lock_type, void* user_data)
{
    (void)pid;
    (void)lock_type;
    int* found = (int*)user_data;
    *found = 1;
            return 1;
}

/* Check if process exists and is alive */
static int active_list_has_process_by_type(LockType filter_type)
{
    int found = 0;
    iterate_active_processes(check_exists_callback, &found, filter_type);
    return found;
}

/* Callback for counting (just increment and continue) */
static int count_callback(pid_t pid, LockType lock_type, void* user_data)
{
    (void)pid;
    (void)lock_type;
    int* count = (int*)user_data;
    (*count)++;
    return 0;  /* Continue iteration */
}

/* Count processes by type */
static int active_list_count_processes_by_type(LockType filter_type)
{
    int count = 0;
    iterate_active_processes(count_callback, &count, filter_type);
    return count;
}

/* Callback for formatting list */
typedef struct {
    char* buffer;
    size_t buffer_size;
    size_t pos;
    int first;
} FormatContext;

static int format_list_callback(pid_t pid, LockType lock_type, void* user_data)
{
    FormatContext* ctx = (FormatContext*)user_data;

    if(!ctx->first && ctx->pos < ctx->buffer_size - 1)
    {
        ctx->buffer[ctx->pos++] = ',';
        ctx->buffer[ctx->pos++] = ' ';
    }
    ctx->first = 0;

    const char* type_str = (lock_type == SHARED_LOCK) ? "READ" : "WRITE";
    int written = snprintf(ctx->buffer + ctx->pos, ctx->buffer_size - ctx->pos,
                          "PID %d (%s)", (int)pid, type_str);
    if(written > 0 && (size_t)written < ctx->buffer_size - ctx->pos)
        ctx->pos += (size_t)written;

    return 0;  /* Continue iteration */
}

/* Get list of active processes as formatted string */
static void active_list_get_formatted_list(char* buffer, size_t buffer_size)
{
    FormatContext ctx = {buffer, buffer_size, 0, 1};
    buffer[0] = '\0';

    iterate_active_processes(format_list_callback, &ctx, INVALID_LOCK);

    if(ctx.pos == 0)
    {
        strncpy(buffer, "none", buffer_size - 1);
        buffer[buffer_size - 1] = '\0';
    }
    else
        buffer[ctx.pos] = '\0';
}

/* Clean up dead processes */
static void active_list_cleanup_dead_processes(void)
{
    /* Just iterate through all processes - check_and_cleanup_process will remove dead ones */
    iterate_active_processes(count_callback, NULL, INVALID_LOCK);
}

int count_queue_tasks(void)
{
    return sorted_list_count_tasks();
}


static int count_active_processes_by_type(LockType filter_type)
{
    if((g_active_list == NULL || g_active_sem == NULL) && init_active_processes_list() < 0)
        return 0;

    sem_wait(g_active_sem);
    int count = active_list_count_processes_by_type(filter_type);
    sem_post(g_active_sem);
    return count;
}

int count_active_processes(void)
{
    return count_active_processes_by_type(INVALID_LOCK);
}

int count_active_read_processes(void)
{
    return count_active_processes_by_type(SHARED_LOCK);
}

void get_active_processes_list(char* buffer, size_t buffer_size)
{
    if((g_active_list == NULL || g_active_sem == NULL) && init_active_processes_list() < 0)
        return;

    sem_wait(g_active_sem);
    active_list_get_formatted_list(buffer, buffer_size);
    sem_post(g_active_sem);
}

int get_planned_tasks_count(void)
{
    FILE* planned_file;
    int count = 0;

    planned_file = fopen(PLANNED_TASKS_FILE, "r");
    if(planned_file != NULL)
    {
        if(fscanf(planned_file, "%d", &count) != 1)
            count = 0;
        fclose(planned_file);
    }

    return count;
}

void increment_planned_tasks(void)
{
    FILE* planned_file;
    int queue_fd;
    int count = get_planned_tasks_count();

    queue_fd = open(PLANNED_TASKS_FILE, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if(queue_fd < 0)
        return;

    if(flock(queue_fd, LOCK_EX) < 0)
    {
        close(queue_fd);
        return;
    }

    planned_file = fdopen(queue_fd, "w");
    if(planned_file != NULL)
    {
        fprintf(planned_file, "%d\n", count + 1);
        fflush(planned_file);
        fclose(planned_file);
    }

    flock(queue_fd, LOCK_UN);
    close(queue_fd);
}

int get_completed_tasks_count(void)
{
    FILE* completed_file;
    int count = 0;

    completed_file = fopen(COMPLETED_TASKS_FILE, "r");
    if(completed_file != NULL)
    {
        if(fscanf(completed_file, "%d", &count) != 1)
            count = 0;
        fclose(completed_file);
    }

    return count;
}

void increment_completed_tasks(void)
{
    FILE* completed_file;
    int queue_fd;
    int count = get_completed_tasks_count();

    queue_fd = open(COMPLETED_TASKS_FILE, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if(queue_fd < 0)
        return;

    if(flock(queue_fd, LOCK_EX) < 0)
    {
        close(queue_fd);
        return;
    }

    completed_file = fdopen(queue_fd, "w");
    if(completed_file != NULL)
    {
        fprintf(completed_file, "%d\n", count + 1);
        fflush(completed_file);
        fclose(completed_file);
    }

    flock(queue_fd, LOCK_UN);
    close(queue_fd);
}

void print_statistics(void)
{
    int waiting = 0;
    int active = 0;
    int completed = 0;
    char log_message[LOG_BUFFER_SIZE];

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

int add_task_to_queue_file(pid_t pid, LockType lock_type, const char* data)
{
    char log_message[LOG_BUFFER_SIZE];

    if(sorted_list_add_task(pid, lock_type, data) < 0)
        return -1;

    snprintf(log_message, sizeof(log_message),
             "Task Manager: Task added to queue (PID %d, type: %s)\n",
             (int)pid, lock_type == SHARED_LOCK ? "READ" : "WRITE");
    stream_write_logfile(log_message);

    printf("Task Manager: Task added to queue (PID %d, type: %s)\n",
           (int)pid, lock_type == SHARED_LOCK ? "READ" : "WRITE");

    return 0;
}

int get_task_from_queue_file(pid_t* pid, LockType* lock_type, char* data, size_t data_size)
{
    return sorted_list_get_task(pid, lock_type, data, data_size);
}

// Operation execution

typedef void (*OperationCallback)(pid_t pid, LockInfo* lock, void* user_data);

typedef struct {
    pid_t task_pid;
    LockType lock_type;
    char* task_data;
    int should_fork;  /* 1 for READ (fork), 0 for WRITE (direct execution) */
} TaskContext;

static int try_operation_common(pid_t pid, LockType lock_type,
                                int (*check_blocking)(void),
                                const char* operation_name,
                                OperationCallback execute_op,
                                void* user_data)
{
    LockInfo* lock;
    char log_message[LOG_BUFFER_SIZE];
    char active_list_before[ACTIVE_LIST_SIZE];
    char active_list_after[ACTIVE_LIST_SIZE];

    get_active_processes_list(active_list_before, sizeof(active_list_before));
    int is_blocked = check_blocking();

    if(is_blocked)
    {
        snprintf(log_message, sizeof(log_message),
                 "Task Manager: %s operation for PID %d rejected - blocking processes detected, put back to queue. Blocking processes: %s\n",
                 operation_name, (int)pid, active_list_before);
        stream_write_logfile(log_message);
        return -1;
    }

    lock = try_acquire_lock_nonblocking(lock_type);

    if(lock == NULL)
    {
        snprintf(log_message, sizeof(log_message),
                 "Task Manager: %s operation for PID %d rejected - failed to acquire lock, put back to queue. Blocking processes: %s\n",
                 operation_name, (int)pid, active_list_before);
        stream_write_logfile(log_message);
        return -1;
    }
    add_active_process(pid, lock_type);
    get_active_processes_list(active_list_after, sizeof(active_list_after));

    snprintf(log_message, sizeof(log_message),
             "Task Manager: %s operation for PID %d acquired lock. Active: %s\n",
             operation_name, (int)pid, active_list_after);
    stream_write_logfile(log_message);

    execute_op(pid, lock, user_data);

    release_lock_info(lock);
    remove_active_process(pid);
    increment_completed_tasks();

    snprintf(log_message, sizeof(log_message),
             "Task Manager: %s operation for PID %d completed\n",
             operation_name, (int)pid);
    stream_write_logfile(log_message);

    return 0;
}

static void execute_read_callback(pid_t pid, LockInfo* lock, void* user_data)
{
    (void)user_data;
    read_from_file(pid, lock);
}

static void execute_write_callback(pid_t pid, LockInfo* lock, void* user_data)
{
    char* write_data = NULL;

    if(user_data != NULL)
    {
        const char* provided_data = (const char*)user_data;
        if(strlen(provided_data) > 0)
            write_data = strdup(provided_data);
    }

    if(write_data == NULL)
        write_data = random_data();

    if(write_data != NULL)
    {
        write_to_file(pid, write_data, lock);
        free(write_data);
    }
}

int try_read_operation(pid_t pid)
{
    return try_operation_common(pid, SHARED_LOCK,
                                has_active_write_process,  /* Check for blocking WRITE processes */
                                "READ",
                                execute_read_callback,
                                NULL);
}

int try_write_operation(pid_t pid, const char* data)
{
    return try_operation_common(pid, EXCLUSIVE_LOCK,
                                has_any_active_process,  /* Check for any blocking processes */
                                "WRITE",
                                execute_write_callback,
                                (void*)data);
}

void execute_read_task(pid_t task_pid)
{
    pid_t child_pid = fork();

    if(child_pid < 0)
    {
        perror("fork failed for read task");
        return;
    }

    if(child_pid == 0)
    {
        /* Child process - execute read */
        LockInfo* lock;
        char log_message[LOG_BUFFER_SIZE];
        pid_t my_pid = getpid();

    add_active_process(my_pid, SHARED_LOCK);
    int active_reads = count_active_read_processes();

        snprintf(log_message, sizeof(log_message),
                 "Forked read process %d: Started reading for task PID %d (running simultaneously with %d other active read process(es))\n",
                 (int)my_pid, (int)task_pid, active_reads > 0 ? active_reads - 1 : 0);
        stream_write_logfile(log_message);

        lock = try_acquire_lock_nonblocking(SHARED_LOCK);

        if(lock != NULL)
        {
            read_from_file(task_pid, lock);
            release_lock_info(lock);
            remove_active_process(my_pid);
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

            char active_list[ACTIVE_LIST_SIZE];
            get_active_processes_list(active_list, sizeof(active_list));
            snprintf(log_message, sizeof(log_message),
                     "Forked read process %d: READ task for PID %d rejected - failed to acquire lock. Blocking processes: %s\n",
                     (int)my_pid, (int)task_pid, active_list);
            stream_write_logfile(log_message);

            fprintf(stderr, "Fatal error: Forked read process failed to acquire lock\n");
            exit(1);
        }
    }
    waitpid(child_pid, NULL, WNOHANG);
}

static int process_task_in_worker(TaskContext* ctx)
{
    char log_message[LOG_BUFFER_SIZE];
    char active_list_check[ACTIVE_LIST_SIZE];
    int is_blocked;
    int (*check_blocking)(void);
    const char* operation_name;

    if(ctx->lock_type == SHARED_LOCK)
    {
        check_blocking = has_active_write_process;
        operation_name = "READ";
    }
    else
    {
        check_blocking = has_any_active_process;
        operation_name = "WRITE";
    }

    is_blocked = check_blocking();
    get_active_processes_list(active_list_check, sizeof(active_list_check));
    snprintf(log_message, sizeof(log_message),
             "Worker: Checking active processes for %s task PID %d. Is blocked: %d. Active list: %s\n",
             operation_name, (int)ctx->task_pid, is_blocked, active_list_check);
    stream_write_logfile(log_message);

    if(ctx->lock_type == SHARED_LOCK)
    {
        int recheck_count = 0;
        while (!is_blocked && recheck_count < MAX_RECHECKS)
        {
            usleep(RECHECK_DELAY_US);
            is_blocked = check_blocking();

            if(is_blocked)
            {
                get_active_processes_list(active_list_check, sizeof(active_list_check));
                snprintf(log_message, sizeof(log_message),
                         "Worker: Re-checked active processes for READ task PID %d after delay (attempt %d). Is blocked: %d. Active list: %s\n",
                         (int)ctx->task_pid, recheck_count + 1, is_blocked, active_list_check);
                stream_write_logfile(log_message);
                break;
            }
            recheck_count++;
        }
    }

    if(is_blocked)
    {
        add_task_to_queue_file(ctx->task_pid, ctx->lock_type, ctx->task_data);
        snprintf(log_message, sizeof(log_message),
                 "Worker: %s task for PID %d rejected - blocking processes detected, put back to queue. Blocking processes: %s\n",
                 operation_name, (int)ctx->task_pid, active_list_check);
        stream_write_logfile(log_message);
        return 0;
    }

    if(ctx->should_fork)
    {
        int active_reads_before = count_active_read_processes();
        execute_read_task(ctx->task_pid);
        usleep(FORK_DELAY_US);

        char active_list_after_fork[ACTIVE_LIST_SIZE];
        get_active_processes_list(active_list_after_fork, sizeof(active_list_after_fork));
        int waiting_count = count_queue_tasks();

        snprintf(log_message, sizeof(log_message),
                 "Worker: Forked read process for PID %d (can run simultaneously with %d other active read process(es)). Current active processes: %s. Waiting tasks: %d\n",
                 (int)ctx->task_pid, active_reads_before, active_list_after_fork, waiting_count);
        stream_write_logfile(log_message);
        return 1;
    }
    else
    {
        snprintf(log_message, sizeof(log_message),
                 "Worker: Attempting to acquire EXCLUSIVE lock for WRITE task PID %d\n",
                 (int)ctx->task_pid);
        stream_write_logfile(log_message);

        LockInfo* lock = try_acquire_lock_nonblocking(EXCLUSIVE_LOCK);
        if(lock == NULL)
        {
            snprintf(log_message, sizeof(log_message),
                     "Worker: Failed to acquire lock for WRITE task PID %d\n",
                     (int)ctx->task_pid);
            stream_write_logfile(log_message);
            get_active_processes_list(active_list_check, sizeof(active_list_check));
            add_task_to_queue_file(ctx->task_pid, ctx->lock_type, ctx->task_data);
            snprintf(log_message, sizeof(log_message),
                     "Worker: WRITE task for PID %d rejected - blocking processes detected, put back to queue. Blocking processes: %s\n",
                     (int)ctx->task_pid, active_list_check);
            stream_write_logfile(log_message);
            return 0;
        }

        snprintf(log_message, sizeof(log_message),
                 "Worker: Successfully acquired lock for WRITE task PID %d\n",
                 (int)ctx->task_pid);
        stream_write_logfile(log_message);

        /* Add to active processes BEFORE executing (so others know we're using it) */
        snprintf(log_message, sizeof(log_message),
                 "Worker: Adding WRITE process PID %d to active processes\n",
                 (int)getpid());
        stream_write_logfile(log_message);

        add_active_process(getpid(), EXCLUSIVE_LOCK);
        snprintf(log_message, sizeof(log_message),
                 "Worker: Added WRITE process PID %d to active processes\n",
                 (int)getpid());
        stream_write_logfile(log_message);

        /* Check waiting tasks (for logging) */
        snprintf(log_message, sizeof(log_message),
                 "Worker: Getting active processes list for WRITE task PID %d\n",
                 (int)ctx->task_pid);
        stream_write_logfile(log_message);

        char active_list_after[ACTIVE_LIST_SIZE];
        get_active_processes_list(active_list_after, sizeof(active_list_after));

        snprintf(log_message, sizeof(log_message),
                 "Worker: Counting queue tasks for WRITE task PID %d\n",
                 (int)ctx->task_pid);
        stream_write_logfile(log_message);

        int waiting_count = count_queue_tasks();

        if(waiting_count > 0)
        {
            snprintf(log_message, sizeof(log_message),
                     "Worker: WRITE process PID %d is now active. %d tasks waiting in queue. Active processes: %s\n",
                     (int)getpid(), waiting_count, active_list_after);
            stream_write_logfile(log_message);
        }

        char* write_data = NULL;

        if(strlen(ctx->task_data) > 0)
            write_data = strdup(ctx->task_data);
        else
            write_data = random_data();

        if(write_data != NULL)
        {
            write_to_file(ctx->task_pid, write_data, lock);
            free(write_data);
        }

        release_lock_info(lock);
        remove_active_process(getpid());
        increment_completed_tasks();
        print_statistics();
        return 1;
    }
}

void worker_process(void)
{
    pid_t task_pid;
    LockType lock_type;
    char task_data[MAX_DATA_SIZE];
    char log_message[LOG_BUFFER_SIZE];
    TaskContext ctx;

    printf("Worker process %d: Started, processing queue...\n", (int)getpid());

    while (1)
    {
        static int cleanup_count = 0;
        cleanup_count++;

        if(cleanup_count % CLEANUP_INTERVAL == 0)
            cleanup_active_processes();

        static int stats_count = 0;
        stats_count++;
        if(stats_count % STATS_INTERVAL == 0)
            print_statistics();

        memset(task_data, 0, sizeof(task_data));
        int result = get_task_from_queue_file(&task_pid, &lock_type, task_data, sizeof(task_data));

        if(result == 0)
        {
            ctx.task_pid = task_pid;
            ctx.lock_type = lock_type;
            ctx.task_data = task_data;
            ctx.should_fork = (lock_type == SHARED_LOCK) ? 1 : 0;
            process_task_in_worker(&ctx);
        }
        else
        {
            int planned = get_planned_tasks_count();
            int completed = get_completed_tasks_count();
            int waiting = count_queue_tasks();
            int active = count_active_processes();

            if(waiting == 0 && active == 0 && planned > 0 && completed >= planned)
            {
                snprintf(log_message, sizeof(log_message),
                         "Worker: All tasks completed! Planned: %d, Completed: %d. Exiting...\n",
                         planned, completed);
                stream_write_logfile(log_message);
                printf("Worker: All tasks completed (%d/%d). Exiting...\n", completed, planned);
                break;
            }

            static int empty_count = 0;
            empty_count++;
            if(empty_count % EMPTY_QUEUE_LOG_INTERVAL == 0)
            {
                snprintf(log_message, sizeof(log_message),
                         "Worker: Queue is empty, waiting for tasks... (Planned: %d, Completed: %d, Active: %d)\n",
                         planned, completed, active);
                stream_write_logfile(log_message);
            }
        }

        if(result != 0)
            sleep(WORKER_SLEEP);
        else
            usleep(POST_TASK_DELAY_US);
    }
}

void task_manager_process(int argc, char* argv[])
{
    pid_t pid = getpid();
    char* write_data = NULL;
    int result;

    srand((unsigned int)(time(NULL) ^ pid));

    if(argc < 2) {
        fprintf(stderr, "Fatal error: Usage: %s <read|write> [data]\n", argv[0]);
        fprintf(stderr, "  read  - add read task to queue\n");
        fprintf(stderr, "  write - add write task to queue\n");
        exit(1);
    }

    if(strcmp(argv[1], "read") == 0)
    {
        increment_planned_tasks();
        result = try_read_operation(pid);
        if(result == 0)
        {
            printf("Task Manager: READ operation completed immediately\n");
            return;
        }
        add_task_to_queue_file(pid, SHARED_LOCK, NULL);
        printf("Task Manager: READ task added to queue (could not execute immediately)\n");
    }
    else if(strcmp(argv[1], "write") == 0)
    {
        write_data = random_data();
        if(write_data == NULL)
        {
            fprintf(stderr, "Fatal error: Failed to generate random data\n");
            exit(1);
        }

        increment_planned_tasks();
        result = try_write_operation(pid, write_data);
        if(result == 0)
        {
            free(write_data);
            printf("Task Manager: WRITE operation completed immediately\n");
            return;
        }
        add_task_to_queue_file(pid, EXCLUSIVE_LOCK, write_data);
        free(write_data);
        printf("Task Manager: WRITE task added to queue (could not execute immediately)\n");
    }
    else
    {
        fprintf(stderr, "Fatal error: Unknown command: %s\n", argv[1]);
        exit(1);
    }
}

int main(int argc, char* argv[])
{

    if(init_queue_sorted_list() < 0)
    {
        fprintf(stderr, "Fatal error: Failed to initialize queue sorted list\n");
        exit(1);
    }

    if(init_active_processes_list() < 0)
    {
        fprintf(stderr, "Fatal error: Failed to initialize active processes list\n");
        exit(1);
    }

    atexit(cleanup_queue_sorted_list);
    atexit(cleanup_active_processes_list);

    if(argc >= 2 && strcmp(argv[1], "worker") == 0)
    {
        worker_process();
        return 0;
    }

    task_manager_process(argc, argv);
    return 0;
}
