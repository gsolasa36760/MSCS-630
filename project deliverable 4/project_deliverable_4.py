import os
import sys
import subprocess
import signal
import time
import threading
import heapq
import collections
import random # For simulating page access and producer-consumer
import io # To capture stdout for built-in commands

# ----------------- Global Configuration for Paging ----------------- #
PAGE_SIZE = 4  # Size of each page frame (e.g., 4 units/KB)
MAX_MEMORY_PAGES = 10  # Total number of page frames available in memory
memory = [(None, None) for _ in range(MAX_MEMORY_PAGES)]  # Represents physical memory: (process_id, virtual_page_number)
memory_lock = threading.Lock() # Lock for protecting memory access

process_memory_map = collections.defaultdict(list) # Maps process_id to a list of (physical_frame_index, virtual_page_number) tuples
page_fault_counts = collections.defaultdict(int) # Tracks page faults per process

# For FIFO page replacement
fifo_queue = collections.deque() # Stores (physical_frame_index, process_id, virtual_page_number) in order of loading

# For LRU page replacement
lru_tracker = {} # Stores last access time for each physical frame: {physical_frame_index: timestamp}
lru_lock = threading.Lock() # Lock for LRU tracker updates

# ----------------- Global Configuration for Synchronization ----------------- #
shared_resource_lock = threading.Lock() # Mutex for general shared resource access

# Producer-Consumer problem setup
BUFFER_SIZE = 5
shared_buffer = collections.deque(maxlen=BUFFER_SIZE)
buffer_lock = threading.Lock()
buffer_full = threading.Semaphore(0) # Initially no items in buffer
buffer_empty = threading.Semaphore(BUFFER_SIZE) # Initially buffer has BUFFER_SIZE empty slots

# ----------------- Job Tracking ----------------- #
jobs = {}
job_counter = 1

class Job:
    def __init__(self, job_id, process, command, priority=0, memory_pages=0):
        self.job_id = job_id
        self.process = process
        self.command = command
        self.priority = priority
        self.status = "Running"
        self.arrival_time = time.time()
        self.start_time = None
        self.finish_time = None
        self.memory_pages = memory_pages # Number of pages requested by this job

    def waiting_time(self):
        if self.start_time is not None:
            return round(self.start_time - self.arrival_time, 2)
        return None

    def turnaround_time(self):
        if self.finish_time is not None:
            return round(self.finish_time - self.arrival_time, 2)
        return None

    def response_time(self):
        if self.start_time is not None:
            return round(self.start_time - self.arrival_time, 2)
        return None

# ----------------- User Authentication and Permissions ----------------- #

USERS = {
    "admin": {"password": "adminpass", "role": "admin"},
    "user1": {"password": "user1pass", "role": "standard"},
    "guest": {"password": "guestpass", "role": "guest"}
}

# Define file permissions: {filename: {role: [permissions]}}
# Permissions: 'r' (read), 'w' (write), 'x' (execute)
# For simplicity, these are hardcoded. In a real system, they would be dynamic.
FILE_PERMISSIONS = {
    "system_file.conf": {"admin": ["r", "w"], "standard": ["r"], "guest": ["r"]},
    "user_data.txt": {"admin": ["r", "w"], "standard": ["r", "w"], "guest": ["r"]},
    "admin_script.sh": {"admin": ["r", "x"], "standard": ["r"], "guest": []},
    "guest_document.txt": {"admin": ["r", "w"], "standard": ["r"], "guest": ["r", "w"]},
    "public_file.txt": {"admin": ["r", "w"], "standard": ["r", "w"], "guest": ["r", "w"]},
}

current_user = None # Stores the logged-in user's data

def login():
    global current_user
    while True:
        username = input("Username: ")
        password = input("Password: ")
        if username in USERS and USERS[username]["password"] == password:
            current_user = {"username": username, "role": USERS[username]["role"]}
            print(f"Welcome, {username} ({current_user['role']}).")
            break
        else:
            print("Invalid credentials. Please try again.")

def check_file_permission(filename, permission_type):
    """
    Checks if the current user has the required permission for a file.
    """
    if current_user is None:
        print("Error: No user logged in.")
        return False

    if filename not in FILE_PERMISSIONS:
        # If file permissions are not explicitly defined, assume default permissions
        # For simplicity, let's say all users have read/write/execute if not specified.
        return True

    user_role = current_user["role"]
    allowed_permissions = FILE_PERMISSIONS[filename].get(user_role, [])

    if permission_type in allowed_permissions:
        return True
    else:
        print(f"Permission Denied: User '{current_user['username']}' ({user_role}) does not have '{permission_type}' permission for '{filename}'.")
        return False

# ----------------- Memory Management Functions ----------------- #

def _find_free_page_frame():
    """Finds an empty page frame in memory."""
    with memory_lock:
        for i, (pid, vpn) in enumerate(memory):
            if pid is None:
                return i
    return -1 # No free frame found

def _replace_page_fifo(process_id, virtual_page_to_load):
    """
    Implements the FIFO page replacement algorithm.
    Removes the oldest page from memory and loads the new one.
    """
    global memory, fifo_queue, process_memory_map, lru_tracker

    if not fifo_queue:
        print("FIFO queue is empty, cannot replace page.")
        return False

    with memory_lock:
        frame_to_replace_idx, old_pid, old_vpn = fifo_queue.popleft()

        print(f"Memory Full! FIFO: Replacing page ({old_pid}, {old_vpn}) from frame {frame_to_replace_idx} to load ({process_id}, {virtual_page_to_load}).")

        # Remove the old page's entry from its process's memory map
        if (frame_to_replace_idx, old_vpn) in process_memory_map[old_pid]:
            process_memory_map[old_pid].remove((frame_to_replace_idx, old_vpn))

        # Update memory
        memory[frame_to_replace_idx] = (process_id, virtual_page_to_load)

        # Add the new page to the FIFO queue
        fifo_queue.append((frame_to_replace_idx, process_id, virtual_page_to_load))

        # Update LRU tracker if it was tracking the replaced page (not strictly necessary for FIFO, but good for consistency)
        with lru_lock:
            if frame_to_replace_idx in lru_tracker:
                del lru_tracker[frame_to_replace_idx]
            lru_tracker[frame_to_replace_idx] = time.time()

        return frame_to_replace_idx

def _replace_page_lru(process_id, virtual_page_to_load):
    """
    Implements the LRU page replacement algorithm.
    Removes the least recently used page from memory and loads the new one.
    """
    global memory, lru_tracker, process_memory_map, fifo_queue

    if not lru_tracker:
        print("LRU tracker is empty, cannot replace page.")
        return False

    with memory_lock:
        with lru_lock:
            # Find the least recently used page frame
            least_recent_frame_idx = min(lru_tracker, key=lru_tracker.get)
            old_pid, old_vpn = memory[least_recent_frame_idx]

            print(f"Memory Full! LRU: Replacing page ({old_pid}, {old_vpn}) from frame {least_recent_frame_idx} to load ({process_id}, {virtual_page_to_load}).")

            # Remove the old page's entry from its process's memory map
            if (least_recent_frame_idx, old_vpn) in process_memory_map[old_pid]:
                process_memory_map[old_pid].remove((least_recent_frame_idx, old_vpn))

            # Update memory
            memory[least_recent_frame_idx] = (process_id, virtual_page_to_load)

            # Update LRU tracker for the new page
            lru_tracker[least_recent_frame_idx] = time.time()

            # Remove the old page from FIFO queue if it exists (for consistency if both algorithms are used)
            if (least_recent_frame_idx, old_pid, old_vpn) in fifo_queue:
                fifo_queue.remove((least_recent_frame_idx, old_pid, old_vpn))
            # Add the new page to the FIFO queue (even if not FIFO replacement, for consistency)
            fifo_queue.append((least_recent_frame_idx, process_id, virtual_page_to_load))

        return least_recent_frame_idx

def allocate_pages(process_id, num_pages):
    """
    Allocates a specified number of pages for a process.
    Attempts to use free frames first, then triggers page replacement if memory is full.
    """
    global memory, process_memory_map, fifo_queue, lru_tracker

    allocated_pages = 0
    pages_to_allocate = [] # Store virtual page numbers that need allocation
    for i in range(num_pages):
        pages_to_allocate.append(i) # Assuming virtual pages 0 to num_pages-1 for simplicity

    for virtual_page_number in pages_to_allocate:
        frame_idx = _find_free_page_frame()
        if frame_idx != -1:
            with memory_lock:
                memory[frame_idx] = (process_id, virtual_page_number)
                process_memory_map[process_id].append((frame_idx, virtual_page_number))
                fifo_queue.append((frame_idx, process_id, virtual_page_number))
                with lru_lock:
                    lru_tracker[frame_idx] = time.time()
                allocated_pages += 1
                print(f"Process {process_id}: Allocated page {virtual_page_number} to frame {frame_idx}.")
        else:
            print(f"Memory full for process {process_id} requiring {num_pages - allocated_pages} more pages. Attempting page replacement...")
            # Here, we can choose a default replacement algorithm or make it configurable
            # For simplicity, let's always try LRU for initial allocation if full
            replaced_frame_idx = _replace_page_lru(process_id, virtual_page_number) # Or _replace_page_fifo
            if replaced_frame_idx is not False:
                process_memory_map[process_id].append((replaced_frame_idx, virtual_page_number))
                allocated_pages += 1
            else:
                print(f"Could not allocate page {virtual_page_number} for process {process_id}. No replacement candidate found.")
                break # Stop if replacement failed

    return allocated_pages

def deallocate_pages(process_id):
    """Deallocates all pages associated with a process."""
    global memory, process_memory_map, fifo_queue, lru_tracker
    with memory_lock:
        if process_id in process_memory_map:
            for frame_idx, virtual_page_number in process_memory_map[process_id]:
                memory[frame_idx] = (None, None)
                # Remove from FIFO queue
                if (frame_idx, process_id, virtual_page_number) in fifo_queue:
                    fifo_queue.remove((frame_idx, process_id, virtual_page_number))
                # Remove from LRU tracker
                with lru_lock:
                    if frame_idx in lru_tracker:
                        del lru_tracker[frame_idx]
            print(f"Process {process_id}: Deallocated all pages.")
            del process_memory_map[process_id]
        else:
            print(f"Process {process_id} has no allocated memory.")

def access_page(process_id, virtual_page_number, algorithm='lru'):
    """
    Simulates a process accessing a page. Handles page faults and replacement.
    """
    global memory, process_memory_map, page_fault_counts, fifo_queue, lru_tracker

    page_found = False
    physical_frame_idx = -1

    with memory_lock:
        for frame_idx, (pid, vpn) in enumerate(memory):
            if pid == process_id and vpn == virtual_page_number:
                page_found = True
                physical_frame_idx = frame_idx
                break

    if page_found:
        print(f"Process {process_id}: Page {virtual_page_number} found in memory at frame {physical_frame_idx}.")
        with lru_lock:
            lru_tracker[physical_frame_idx] = time.time() # Update access time for LRU
    else:
        page_fault_counts[process_id] += 1
        print(f"Process {process_id}: PAGE FAULT for page {virtual_page_number}! Count: {page_fault_counts[process_id]}")

        frame_idx = _find_free_page_frame()
        if frame_idx != -1:
            with memory_lock:
                memory[frame_idx] = (process_id, virtual_page_number)
                process_memory_map[process_id].append((frame_idx, virtual_page_number))
                fifo_queue.append((frame_idx, process_id, virtual_page_number))
                with lru_lock:
                    lru_tracker[frame_idx] = time.time()
            print(f"Process {process_id}: Loaded page {virtual_page_number} into free frame {frame_idx}.")
        else:
            print("Memory is full. Initiating page replacement...")
            replaced_frame_idx = -1
            if algorithm == 'fifo':
                replaced_frame_idx = _replace_page_fifo(process_id, virtual_page_number)
            elif algorithm == 'lru':
                replaced_frame_idx = _replace_page_lru(process_id, virtual_page_number)
            else:
                print("Invalid page replacement algorithm specified.")
                return False

            if replaced_frame_idx is not False:
                # Add the new page to the process's memory map if it's not already there (could be if it was replaced)
                # Ensure the entry is updated if it was an existing page but replaced
                if (replaced_frame_idx, virtual_page_number) not in process_memory_map[process_id]:
                    process_memory_map[process_id].append((replaced_frame_idx, virtual_page_number))
                print(f"Process {process_id}: Page {virtual_page_number} loaded into frame {replaced_frame_idx} via {algorithm.upper()} replacement.")
            else:
                print(f"Process {process_id}: Failed to load page {virtual_page_number} even after replacement attempt.")
                return False
    return True

# ----------------- Synchronization Problem: Producer-Consumer ----------------- #

def producer(items_to_produce):
    """
    Producer function: Adds items to the shared buffer.
    """
    for i in range(items_to_produce):
        item = f"item_{i}"
        buffer_empty.acquire() # Wait if buffer is full
        with buffer_lock:
            shared_buffer.append(item)
            print(f"Producer: Produced {item}. Buffer: {list(shared_buffer)}")
        buffer_full.release() # Signal that buffer has an item
        time.sleep(random.uniform(0.1, 0.5))

def consumer(items_to_consume):
    """
    Consumer function: Removes items from the shared buffer.
    """
    for _ in range(items_to_consume):
        buffer_full.acquire() # Wait if buffer is empty
        with buffer_lock:
            item = shared_buffer.popleft()
            print(f"Consumer: Consumed {item}. Buffer: {list(shared_buffer)}")
        buffer_empty.release() # Signal that buffer has an empty slot
        time.sleep(random.uniform(0.1, 0.7))

# ----------------- Built-in Commands ----------------- #
# Modified built-ins to return output as string for piping
def cd(args):
    try:
        os.chdir(args[1])
        return "" # No output
    except IndexError:
        return "cd: missing operand"
    except FileNotFoundError:
        return "cd: no such directory"

def pwd(args):
    return os.getcwd()

def echo(args):
    return " ".join(args[1:])

def clear(args):
    os.system("clear")
    return "" # No output

def ls(args):
    return "\n".join(os.listdir(os.getcwd()))

def cat(args, input_stream=None):
    """
    Reads file content or input stream.
    Args:
        args: Command arguments (e.g., ['cat', 'filename'])
        input_stream: A string from previous pipe stage.
    Returns:
        Content as a string.
    """
    if input_stream:
        return input_stream # If input is piped, just pass it through for now or if we want to add more features.
    
    try:
        filename = args[1]
        if not check_file_permission(filename, 'r'):
            return "" # Return empty string on permission denied
        with open(filename, 'r') as f:
            return f.read()
    except IndexError:
        return "cat: missing filename"
    except Exception as e:
        return f"cat: {e}"

def grep(args, input_stream=None):
    """
    Filters lines based on a pattern.
    Args:
        args: Command arguments (e.g., ['grep', 'pattern', 'filename'])
        input_stream: A string from previous pipe stage.
    Returns:
        Filtered lines as a string.
    """
    if len(args) < 2 and not input_stream:
        return "grep: missing pattern and/or input"

    pattern = args[1]
    lines_to_search = []

    if input_stream:
        lines_to_search = input_stream.splitlines()
    elif len(args) > 2: # Filename provided
        filename = args[2]
        if not check_file_permission(filename, 'r'):
            return ""
        try:
            with open(filename, 'r') as f:
                lines_to_search = f.read().splitlines()
        except FileNotFoundError:
            return f"grep: {filename}: No such file or directory"
        except Exception as e:
            return f"grep: {e}"
    else:
        # If no input stream and no filename, read from stdin (interactive input)
        # For simplicity in this simulation, we'll assume direct input if not piped or from file.
        # In a real shell, this would block and wait for user input.
        return "grep: No input provided. Use 'command | grep pattern' or 'grep pattern filename'."


    filtered_lines = [line for line in lines_to_search if pattern in line]
    return "\n".join(filtered_lines)


def mkdir(args):
    try:
        dirname = args[1]
        if not check_file_permission(os.getcwd(), 'w'): # Check write permission on current dir
            return "" # Return empty string on permission denied
        os.mkdir(dirname)
        return ""
    except IndexError:
        return "mkdir: missing directory name"
    except Exception as e:
        return f"mkdir: {e}"

def rmdir(args):
    try:
        dirname = args[1]
        if not check_file_permission(os.getcwd(), 'w'): # Check write permission on current dir
            return "" # Return empty string on permission denied
        os.rmdir(dirname)
        return ""
    except IndexError:
        return "rmdir: missing directory name"
    except Exception as e:
        return f"rmdir: {e}"

def rm(args):
    try:
        filename = args[1]
        if not check_file_permission(filename, 'w'): # Assume 'w' for delete
            return "" # Return empty string on permission denied
        os.remove(filename)
        if filename in FILE_PERMISSIONS:
            del FILE_PERMISSIONS[filename] # Remove from our simulated permissions
        return ""
    except IndexError:
        return "rm: missing filename"
    except Exception as e:
        return f"rm: {e}"

def touch(args):
    try:
        filename = args[1]
        if os.path.exists(filename) and not check_file_permission(filename, 'w'):
            return "" # If exists, need write permission to touch
        if not os.path.exists(filename) and not check_file_permission(os.getcwd(), 'w'):
            return "" # If new, need write permission on current dir to create

        with open(filename, 'a'):
            os.utime(filename, None)
        # For a new file created by touch, set some default permissions
        if filename not in FILE_PERMISSIONS:
            FILE_PERMISSIONS[filename] = {
                current_user["role"]: ["r", "w"],
                "admin": ["r", "w"],
                "standard": ["r"], # Other users might have read only by default
                "guest": ["r"]
            }
        return ""
    except IndexError:
        return "touch: missing filename"
    except Exception as e:
        return f"touch: {e}"

def kill_proc(args):
    try:
        pid = int(args[1])
        if current_user["role"] != "admin":
            killed_own_process = False
            for jid, job in jobs.items():
                if job.process.pid == pid and getattr(job, 'command_user', None) == current_user["username"]:
                    killed_own_process = True
                    break
            if not killed_own_process:
                return f"Permission Denied: User '{current_user['username']}' cannot kill process {pid}."

        if sys.platform != "win32":
            os.killpg(os.getpgid(pid), signal.SIGTERM)
        else:
            os.kill(pid, signal.SIGTERM)
        deallocate_pages(pid)
        return ""
    except IndexError:
        return "kill: missing PID"
    except ProcessLookupError:
        return f"kill: No such process with PID {pid}"
    except Exception as e:
        return f"kill: {e}"

def exit_shell(args):
    print("Exiting shell.")
    os._exit(0)
    return "" # Should not be reached

def jobs_cmd(args):
    output_lines = []
    for jid, job in jobs.items():
        status = job.status if job.process.poll() is None else "Done"
        output_lines.append(f"[{jid}] {job.process.pid} {status} {job.command}")
    return "\n".join(output_lines)

def fg(args):
    try:
        jid = int(args[1])
        job = jobs.pop(jid)
        if job.process.poll() is None:
            if current_user["role"] != "admin" and getattr(job, 'command_user', None) != current_user["username"]:
                jobs[jid] = job # Put it back if denied
                return f"Permission Denied: User '{current_user['username']}' cannot bring job [{jid}] to foreground."
            
            output_msg = f"Bringing job [{jid}] {job.command} to foreground\n"
            if sys.platform != "win32":
                os.killpg(os.getpgid(job.process.pid), signal.SIGCONT)
            job.process.wait()
            return output_msg
        else:
            deallocate_pages(job.process.pid)
            return f"Job [{jid}] has already completed"
    except KeyError:
        return f"fg: no such job [{args[1]}]"
    except Exception as e:
        return f"fg: {e}"

def bg(args):
    try:
        jid = int(args[1])
        job = jobs[jid]
        if job.process.poll() is None:
            if current_user["role"] != "admin" and getattr(job, 'command_user', None) != current_user["username"]:
                return f"Permission Denied: User '{current_user['username']}' cannot background job [{jid}]."
            if sys.platform != "win32":
                os.killpg(os.getpgid(job.process.pid), signal.SIGCONT)
            return f"[{jid}] {job.process.pid} resumed in background"
        else:
            deallocate_pages(job.process.pid)
            return f"Job [{jid}] is already done"
    except KeyError:
        return f"bg: no such job [{args[1]}]"
    except Exception as e:
        return f"bg: {e}"

# New built-in: Memory Information
def meminfo(args):
    output = []
    output.append("\n--- Memory Status ---")
    output.append(f"Page Size: {PAGE_SIZE} units")
    output.append(f"Max Memory Pages: {MAX_MEMORY_PAGES}")
    output.append("\nPhysical Memory Layout (Frame: (PID, VPN))")
    for i, (pid, vpn) in enumerate(memory):
        output.append(f"  Frame {i:2}: {'Free' if pid is None else f'({pid}, {vpn})'}")

    output.append("\nProcess Memory Usage:")
    total_used_memory = 0
    for pid, pages in process_memory_map.items():
        if pid in jobs and jobs[pid].process.poll() is not None:
            deallocate_pages(pid)
            continue
        mem_used = len(pages) * PAGE_SIZE
        total_used_memory += mem_used
        output.append(f"  Process {pid}: {len(pages)} pages ({mem_used} units)")
    output.append(f"\nTotal Memory Used: {total_used_memory} units / {MAX_MEMORY_PAGES * PAGE_SIZE} units")

    output.append("\nPage Fault Counts:")
    for pid, faults in page_fault_counts.items():
        if pid in jobs and jobs[pid].process.poll() is not None:
            continue
        output.append(f"  Process {pid}: {faults} faults")
    output.append("---------------------\n")
    return "\n".join(output)

# New built-in: Access Page
def accesspage(args):
    try:
        process_id = int(args[1])
        virtual_page_number = int(args[2])
        algorithm = args[3].lower() if len(args) > 3 else 'lru'
        if algorithm not in ['fifo', 'lru']:
            algorithm = 'lru'
            return "Invalid algorithm. Choose 'fifo' or 'lru'. Defaulting to LRU."

        if process_id not in jobs or jobs[process_id].process.poll() is not None:
            return f"Error: Process {process_id} not found or not running."

        access_page(process_id, virtual_page_number, algorithm)
        return "" # Output is handled by access_page printing directly
    except IndexError:
        return "Usage: accesspage <process_id> <virtual_page_number> [fifo|lru]"
    except ValueError:
        return "Invalid process ID or page number."
    except Exception as e:
        return f"Error accessing page: {e}"

# New built-in: Run Producer-Consumer demonstration
def run_producer_consumer(args):
    try:
        num_producers = int(args[1])
        num_consumers = int(args[2])
        items_per_producer = int(args[3])
        items_per_consumer = int(args[4])

        producer_threads = []
        consumer_threads = []

        print(f"\n--- Starting Producer-Consumer Simulation (Buffer Size: {BUFFER_SIZE}) ---")

        for i in range(num_producers):
            p_thread = threading.Thread(target=producer, args=(items_per_producer,), name=f"Producer-{i+1}")
            producer_threads.append(p_thread)
            p_thread.start()

        for i in range(num_consumers):
            c_thread = threading.Thread(target=consumer, args=(items_to_consumer,), name=f"Consumer-{i+1}")
            consumer_threads.append(c_thread)
            c_thread.start()

        for t in producer_threads:
            t.join()
        for t in consumer_threads:
            t.join()

        return "--- Producer-Consumer Simulation Finished ---\n"

    except IndexError:
        return "Usage: run_producer_consumer <num_producers> <num_consumers> <items_per_producer> <items_per_consumer>"
    except ValueError:
        return "Invalid number for arguments."
    except Exception as e:
        return f"Error in Producer-Consumer simulation: {e}"

# ----------------- Scheduling Infrastructure ----------------- #
SCHEDULERS = {'rr': 'Round-Robin', 'pbs': 'Priority-Based'}
current_scheduler = None
quantum = 1
scheduler_thread = None
scheduler_event = threading.Event()
rr_queue = []
pbs_queue = []
queue_lock = threading.Lock()

def round_robin_scheduler():
    global rr_queue, quantum
    while not scheduler_event.is_set():
        with queue_lock:
            if not rr_queue:
                time.sleep(0.1)
                continue
            current_job = rr_queue.pop(0)
            if current_job.start_time is None:
                current_job.start_time = time.time()
            current_job.status = "Running"
        # Print directly as scheduler output isn't piped normally
        print(f"\n[RR] Running job {current_job.job_id} for {quantum}s: {current_job.command}")
        start_time = time.time()
        while time.time() - start_time < quantum:
            if scheduler_event.is_set():
                return
            time.sleep(0.1)
            if current_job.process.poll() is not None:
                deallocate_pages(current_job.process.pid)
                break
        with queue_lock:
            if current_job.process.poll() is None:
                current_job.status = "Waiting"
                rr_queue.append(current_job)
                print(f"[RR] Time slice expired for job {current_job.job_id}")
            else:
                current_job.finish_time = time.time()
                print(f"[RR] Job {current_job.job_id} completed")
                if current_job.job_id in jobs:
                    jobs[current_job.job_id].status = "Done"
                deallocate_pages(current_job.process.pid)

def priority_scheduler():
    global pbs_queue
    current_job = None
    while not scheduler_event.is_set():
        with queue_lock:
            if not pbs_queue:
                time.sleep(0.1)
                continue
            _, _, next_job = heapq.heappop(pbs_queue)
            if next_job.start_time is None:
                next_job.start_time = time.time()
            if current_job is not None and next_job.priority < current_job.priority:
                current_job.status = "Waiting"
                heapq.heappush(pbs_queue, (current_job.priority, current_job.arrival_time, current_job))
                print(f"\n[PBS] Preempting job {current_job.job_id}")
            current_job = next_job
            current_job.status = "Running"
            print(f"\n[PBS] Running job {current_job.job_id} (prio {current_job.priority}): {current_job.command}")
        while current_job.process.poll() is None:
            if scheduler_event.is_set():
                return
            time.sleep(0.1)
            with queue_lock:
                if pbs_queue and pbs_queue[0][0] < current_job.priority:
                    break
        with queue_lock:
            if current_job.process.poll() is not None:
                current_job.finish_time = time.time()
                print(f"[PBS] Job {current_job.job_id} completed")
                if current_job.job_id in jobs:
                    jobs[current_job.job_id].status = "Done"
                deallocate_pages(current_job.process.pid)
                current_job = None

def scheduler(args):
    global current_scheduler, scheduler_thread, quantum, scheduler_event
    if len(args) < 2:
        return f"Current scheduler: {current_scheduler}"
    algorithm = args[1].lower()
    if algorithm not in SCHEDULERS:
        return f"Invalid scheduler. Available: {', '.join(SCHEDULERS.keys())}"
    if scheduler_thread is not None:
        scheduler_event.set()
        scheduler_thread.join()
        scheduler_event.clear()
    current_scheduler = algorithm
    if algorithm == 'rr':
        try:
            quantum = float(args[2]) if len(args) > 2 else 1
        except ValueError:
            quantum = 1
            return "Invalid quantum. Using default 1s"
        print(f"Using Round-Robin scheduler with {quantum}s quantum")
        with queue_lock:
            rr_queue.clear()
            rr_queue.extend([job for job in jobs.values() if job.status == "Running"])
        scheduler_thread = threading.Thread(target=round_robin_scheduler, daemon=True)
    else:
        print("Using Priority-Based scheduler")
        with queue_lock:
            pbs_queue.clear()
            for job in jobs.values():
                if job.status == "Running":
                    heapq.heappush(pbs_queue, (job.priority, job.arrival_time, job))
        scheduler_thread = threading.Thread(target=priority_scheduler, daemon=True)
    scheduler_thread.start()
    return "" # No direct output

def addjob(args):
    global job_counter
    try:
        priority = int(args[1])
        memory_pages = int(args[2])
        command = args[3:]

        allocated = allocate_pages(job_counter, memory_pages)
        if allocated < memory_pages:
            return f"Warning: Only allocated {allocated} out of {memory_pages} requested pages for job. Not launching process."

        if command and command[0] not in builtins:
            command_path = command[0]
            if not os.path.isabs(command_path):
                command_path = os.path.join(os.getcwd(), command_path)
            if not check_file_permission(command_path, 'x'):
                deallocate_pages(job_counter)
                return f"Permission Denied: Cannot execute '{command[0]}'."
        
        popen_kwargs = {}
        if sys.platform != "win32":
            popen_kwargs['preexec_fn'] = os.setpgrp

        process = subprocess.Popen(command, **popen_kwargs)
        new_job = Job(job_counter, process, " ".join(command), priority, memory_pages)
        new_job.command_user = current_user["username"]
        jobs[job_counter] = new_job
        with queue_lock:
            if current_scheduler == 'rr':
                rr_queue.append(new_job)
            elif current_scheduler == 'pbs':
                heapq.heappush(pbs_queue, (priority, new_job.arrival_time, new_job))
        return f"[{job_counter}] {process.pid} (prio {priority}, {memory_pages} pages) {new_job.command}"
    except (IndexError, ValueError):
        return "Usage: addjob <priority> <memory_pages> <command>"
    except FileNotFoundError:
        return f"{command[0]}: command not found"
    except Exception as e:
        return f"Error adding job: {e}"


def metrics_cmd(args):
    output_lines = []
    output_lines.append(f"{'JobID':<6} {'Command':<20} {'Priority':<8} {'Arrival':<10} {'Start':<10} {'Finish':<10} {'Waiting':<10} {'Turnaround':<12} {'Response':<10}")
    for jid, job in jobs.items():
        output_lines.append(f"{jid:<6} {job.command:<20} {job.priority:<8} "
              f"{round(job.arrival_time,2):<10} "
              f"{round(job.start_time,2) if job.start_time else '-':<10} "
              f"{round(job.finish_time,2) if job.finish_time else '-':<10} "
              f"{job.waiting_time() if job.waiting_time() is not None else '-':<10} "
              f"{job.turnaround_time() if job.turnaround_time() is not None else '-':<12} "
              f"{job.response_time() if job.response_time() is not None else '-':<10}")
    return "\n".join(output_lines)

# ----------------- Built-in Dispatcher ----------------- #
builtins = {
    "cd": cd, "pwd": pwd, "echo": echo, "clear": clear, "ls": ls,
    "cat": cat, "mkdir": mkdir, "rmdir": rmdir, "rm": rm, "touch": touch,
    "kill": kill_proc, "exit": exit_shell, "jobs": jobs_cmd,
    "fg": fg, "bg": bg, "scheduler": scheduler, "addjob": addjob, "metrics": metrics_cmd,
    "meminfo": meminfo, "accesspage": accesspage, "run_producer_consumer": run_producer_consumer,
    "grep": grep # New built-in
}

# ----------------- Execute Commands ----------------- #
def execute_command(cmd_tokens, background):
    global job_counter
    if not cmd_tokens:
        return

    pipelines = []
    current_pipeline = []
    for token in cmd_tokens:
        if token == '|':
            pipelines.append(current_pipeline)
            current_pipeline = []
        else:
            current_pipeline.append(token)
    pipelines.append(current_pipeline)

    # Handle output redirection for the entire pipeline's final output
    redirect_filename = None
    redirect_mode = None
    final_output_target = None # Can be file object or None (stdout)

    last_cmd_tokens = pipelines[-1]
    if '>' in last_cmd_tokens or '>>' in last_cmd_tokens:
        try:
            idx = -1
            if '>>' in last_cmd_tokens:
                idx = last_cmd_tokens.index('>>')
                redirect_mode = 'a'
            elif '>' in last_cmd_tokens:
                idx = last_cmd_tokens.index('>')
                redirect_mode = 'w'
            
            redirect_filename = last_cmd_tokens[idx + 1]
            pipelines[-1] = last_cmd_tokens[:idx] # Remove redirection part from last command

            # Permission check for output redirection file
            if os.path.exists(redirect_filename) and not check_file_permission(redirect_filename, 'w'):
                print(f"myshell: Permission Denied: Cannot write to '{redirect_filename}'.")
                return
            elif not os.path.exists(redirect_filename) and not check_file_permission(os.getcwd(), 'w'):
                print(f"myshell: Permission Denied: Cannot create '{redirect_filename}' in current directory.")
                return
            
            # Open the file now if it's not a background process
            if not background:
                final_output_target = open(redirect_filename, redirect_mode)

        except (IndexError, ValueError):
            print("myshell: Syntax error in redirection")
            return
        except Exception as e:
            print(f"myshell: Error with redirection setup: {e}")
            return


    input_data = None # String data passed between built-ins or to external
    processes = []
    
    for i, cmd_part_tokens in enumerate(pipelines):
        if not cmd_part_tokens:
            print("myshell: Invalid null command in pipe.")
            # Ensure any open file descriptors or processes are cleaned up
            if final_output_target and not background: final_output_target.close()
            for p in processes:
                if p.poll() is None: p.terminate()
            return

        command_name = cmd_part_tokens[0]
        is_builtin = command_name in builtins

        # Permission check for execution
        if not is_builtin and command_name != '': # external command
            command_path = command_name
            if not os.path.isabs(command_path):
                command_path = os.path.join(os.getcwd(), command_path)
            if not check_file_permission(command_path, 'x'):
                print(f"myshell: Cannot execute '{command_name}' due to permissions. Aborting pipe.")
                if final_output_target and not background: final_output_target.close()
                for p in processes:
                    if p.poll() is None: p.terminate()
                return

        stdout_pipe = subprocess.PIPE if i < len(pipelines) - 1 else None # Output to pipe unless it's the last command
        
        # If it's the last command and there's redirection, set its stdout to the file
        if i == len(pipelines) - 1 and redirect_filename and not background:
            stdout_pipe = final_output_target
        elif i == len(pipelines) - 1 and redirect_filename and background:
            # For background, the file will be opened by subprocess.Popen directly
            stdout_pipe = open(redirect_filename, redirect_mode) # Open here for background process stdout

        try:
            if is_builtin:
                # Call built-in with input_data if available
                # Pass input_data to built-ins that can consume it (like grep, cat if it were to process stdin)
                # Currently, only grep and cat (if args are empty) are designed to consume stdin
                if command_name in ["cat", "grep"]:
                    # These built-ins take an optional input_stream argument
                    result = builtins[command_name](cmd_part_tokens, input_stream=input_data)
                else:
                    # Other built-ins don't consume stdin from a pipe currently
                    result = builtins[command_name](cmd_part_tokens)
                
                if result is not None:
                    input_data = result # Output of current built-in becomes input for next
                else:
                    input_data = "" # No output from built-in
                
                if stdout_pipe: # If this built-in's output should go to a pipe or file
                    if stdout_pipe == sys.stdout or stdout_pipe == sys.stderr: # Direct print if it's the final stdout
                        sys.stdout.write(input_data)
                        sys.stdout.flush()
                    elif isinstance(stdout_pipe, io.TextIOBase) or isinstance(stdout_pipe, io.BufferedWriter): # A file object
                        if isinstance(stdout_pipe, io.TextIOBase):
                            stdout_pipe.write(input_data + "\n")
                        else: # Binary mode for subprocess
                            stdout_pipe.write((input_data + "\n").encode())
                    else: # A pipe (for external command next)
                        # We don't write directly to subprocess.PIPE here, input_data is for the next stage's stdin
                        pass
                else: # No piping, just print to original stdout
                    if i == len(pipelines) - 1: # Only print if it's the last command in the sequence
                        sys.stdout.write(input_data)
                        sys.stdout.flush()

            else: # External command
                # input_data (from previous built-in) must be passed as bytes to Popen's stdin
                stdin_pipe = subprocess.PIPE if input_data else None
                
                popen_kwargs = {'stdin': stdin_pipe, 'stdout': stdout_pipe}
                if sys.platform != "win32":
                    popen_kwargs['preexec_fn'] = os.setpgrp

                process = subprocess.Popen(cmd_part_tokens, **popen_kwargs)
                processes.append(process)

                if stdin_pipe:
                    try:
                        process.stdin.write(input_data.encode())
                        process.stdin.close()
                    except Exception as e:
                        print(f"myshell: Error writing to stdin of {command_name}: {e}")
                        process.terminate()
                        raise # Re-raise to abort the pipeline

                # If this is not the last command, the next command's stdin will be this process's stdout
                if stdout_pipe == subprocess.PIPE:
                    input_data = process.communicate()[0].decode().strip() # Capture output for next stage
                else:
                    input_data = None # No captured output for the next stage if it's going to final_output_target or real stdout

        except FileNotFoundError:
            print(f"{command_name}: command not found")
            if final_output_target and not background: final_output_target.close()
            for p in processes:
                if p.poll() is None: p.terminate()
            return
        except Exception as e:
            print(f"Error executing command '{command_name}': {e}")
            if final_output_target and not background: final_output_target.close()
            for p in processes:
                if p.poll() is None: p.terminate()
            return

    # Wait for the last process in the pipeline to finish if not backgrounded
    if processes:
        if background:
            first_process = processes[0]
            jobs[job_counter] = Job(job_counter, first_process, " | ".join([" ".join(p) for p in pipelines]))
            jobs[job_counter].command_user = current_user["username"]
            print(f"[{job_counter}] {first_process.pid}")
            job_counter += 1
        else:
            for p in processes:
                p.wait()
            for p in processes:
                deallocate_pages(p.pid)
    
    # Close the final output file if it was opened by the shell (not by subprocess.Popen in background)
    if final_output_target and not background:
        final_output_target.close()
    
    # If the last command was a built-in and its output wasn't redirected, print it now
    if is_builtin and not redirect_filename and not background and len(pipelines) == 1: # Only for single built-in commands
        if input_data: # input_data now holds the final result of the built-in
            print(input_data)
        


# ----------------- Main Loop ----------------- #
def shell_loop():
    login() # User must log in first
    while True:
        # Clean up completed jobs and their memory
        pids_to_deallocate = []
        for jid, job in list(jobs.items()): # Use list() to allow modification during iteration
            if job.process.poll() is not None and job.status != "Done":
                print(f"Job [{jid}] {job.command} has finished. Deallocating memory.")
                jobs[jid].status = "Done" # Mark as done to avoid repeated deallocation attempts
                pids_to_deallocate.append(job.process.pid)
        for pid in pids_to_deallocate:
            deallocate_pages(pid)


        try:
            prompt_text = f"myshell [{current_user['username']}@{os.path.basename(os.getcwd())}]> "
            cmd = input(prompt_text)
            if not cmd.strip():
                continue
            
            tokens = cmd.split()
            background = False
            if tokens and tokens[-1] == '&':
                background = True
                tokens = tokens[:-1]
            
            execute_command(tokens, background)
        except KeyboardInterrupt:
            print()
        except EOFError:
            print("\nExiting shell.")
            if scheduler_thread is not None:
                scheduler_event.set()
                scheduler_thread.join()
            os._exit(0)

if __name__ == "__main__":
    shell_loop()
