import os
import sys
import subprocess
import signal
import time
import threading
import heapq

# ----------------- Job Tracking ----------------- #
jobs = {}
job_counter = 1

class Job:
    def __init__(self, job_id, process, command, priority=0):
        self.job_id = job_id
        self.process = process
        self.command = command
        self.priority = priority
        self.status = "Running"
        self.arrival_time = time.time()
        self.start_time = None
        self.finish_time = None

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

# ----------------- Built-in Commands ----------------- #
def cd(args):
    try:
        os.chdir(args[1])
    except IndexError:
        print("cd: missing operand")
    except FileNotFoundError:
        print("cd: no such directory")

def pwd(args):
    print(os.getcwd())

def echo(args):
    print(" ".join(args[1:]))

def clear(args):
    os.system("clear")

def ls(args):
    print("\n".join(os.listdir(os.getcwd())))

def cat(args):
    try:
        with open(args[1], 'r') as f:
            print(f.read())
    except IndexError:
        print("cat: missing filename")
    except Exception as e:
        print(f"cat: {e}")

def mkdir(args):
    try:
        os.mkdir(args[1])
    except IndexError:
        print("mkdir: missing directory name")
    except Exception as e:
        print(f"mkdir: {e}")

def rmdir(args):
    try:
        os.rmdir(args[1])
    except IndexError:
        print("rmdir: missing directory name")
    except Exception as e:
        print(f"rmdir: {e}")

def rm(args):
    try:
        os.remove(args[1])
    except IndexError:
        print("rm: missing filename")
    except Exception as e:
        print(f"rm: {e}")

def touch(args):
    try:
        with open(args[1], 'a'):
            os.utime(args[1], None)
    except IndexError:
        print("touch: missing filename")
    except Exception as e:
        print(f"touch: {e}")

def kill_proc(args):
    try:
        os.kill(int(args[1]), signal.SIGTERM)
    except IndexError:
        print("kill: missing PID")
    except Exception as e:
        print(f"kill: {e}")

def exit_shell(args):
    print("Exiting shell.")
    os._exit(0)

def jobs_cmd(args):
    for jid, job in jobs.items():
        status = job.status if job.process.poll() is None else "Done"
        print(f"[{jid}] {job.process.pid} {status} {job.command}")

def fg(args):
    try:
        jid = int(args[1])
        job = jobs.pop(jid)
        if job.process.poll() is None:
            print(f"Bringing job [{jid}] {job.command} to foreground")
            os.killpg(os.getpgid(job.process.pid), signal.SIGCONT)
            job.process.wait()
        else:
            print(f"Job [{jid}] has already completed")
    except KeyError:
        print(f"fg: no such job [{args[1]}]")
    except Exception as e:
        print(f"fg: {e}")

def bg(args):
    try:
        jid = int(args[1])
        job = jobs[jid]
        if job.process.poll() is None:
            os.killpg(os.getpgid(job.process.pid), signal.SIGCONT)
            print(f"[{jid}] {job.process.pid} resumed in background")
        else:
            print(f"Job [{jid}] is already done")
    except KeyError:
        print(f"bg: no such job [{args[1]}]")
    except Exception as e:
        print(f"bg: {e}")

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
        print(f"\n[RR] Running job {current_job.job_id} for {quantum}s: {current_job.command}")
        start_time = time.time()
        while time.time() - start_time < quantum:
            if scheduler_event.is_set():
                return
            time.sleep(0.1)
            if current_job.process.poll() is not None:
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
                current_job = None

def scheduler(args):
    global current_scheduler, scheduler_thread, quantum, scheduler_event
    if len(args) < 2:
        print(f"Current scheduler: {current_scheduler}")
        return
    algorithm = args[1].lower()
    if algorithm not in SCHEDULERS:
        print(f"Invalid scheduler. Available: {', '.join(SCHEDULERS.keys())}")
        return
    if scheduler_thread is not None:
        scheduler_event.set()
        scheduler_thread.join()
        scheduler_event.clear()
    current_scheduler = algorithm
    if algorithm == 'rr':
        try:
            quantum = float(args[2]) if len(args) > 2 else 1
        except ValueError:
            print("Invalid quantum. Using default 1s")
            quantum = 1
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

def addjob(args):
    global job_counter
    try:
        priority = int(args[1])
        command = args[2:]
        process = subprocess.Popen(command, preexec_fn=os.setpgrp)
        new_job = Job(job_counter, process, " ".join(command), priority)
        jobs[job_counter] = new_job
        with queue_lock:
            if current_scheduler == 'rr':
                rr_queue.append(new_job)
            elif current_scheduler == 'pbs':
                heapq.heappush(pbs_queue, (priority, new_job.arrival_time, new_job))
        print(f"[{job_counter}] {process.pid} (prio {priority}) {new_job.command}")
        job_counter += 1
    except (IndexError, ValueError):
        print("Usage: addjob <priority> <command>")

def metrics_cmd(args):
    print(f"{'JobID':<6} {'Command':<20} {'Priority':<8} {'Arrival':<10} {'Start':<10} {'Finish':<10} {'Waiting':<10} {'Turnaround':<12} {'Response':<10}")
    for jid, job in jobs.items():
        print(f"{jid:<6} {job.command:<20} {job.priority:<8} "
              f"{round(job.arrival_time,2):<10} "
              f"{round(job.start_time,2) if job.start_time else '-':<10} "
              f"{round(job.finish_time,2) if job.finish_time else '-':<10} "
              f"{job.waiting_time() if job.waiting_time() is not None else '-':<10} "
              f"{job.turnaround_time() if job.turnaround_time() is not None else '-':<12} "
              f"{job.response_time() if job.response_time() is not None else '-':<10}")

# ----------------- Built-in Dispatcher ----------------- #
builtins = {
    "cd": cd, "pwd": pwd, "echo": echo, "clear": clear, "ls": ls,
    "cat": cat, "mkdir": mkdir, "rmdir": rmdir, "rm": rm, "touch": touch,
    "kill": kill_proc, "exit": exit_shell, "jobs": jobs_cmd,
    "fg": fg, "bg": bg, "scheduler": scheduler, "addjob": addjob, "metrics": metrics_cmd
}

# ----------------- Execute Commands ----------------- #
def execute_command(cmd_tokens, background):
    global job_counter
    if not cmd_tokens:
        return
    # Handle output redirection
    if '>' in cmd_tokens or '>>' in cmd_tokens:
        redirect_type = '>>' if '>>' in cmd_tokens else '>'
        try:
            idx = cmd_tokens.index(redirect_type)
            command_part = cmd_tokens[:idx]
            filename = cmd_tokens[idx + 1]
            mode = 'a' if redirect_type == '>>' else 'w'
            if command_part[0] == 'echo':
                with open(filename, mode) as f:
                    f.write(" ".join(command_part[1:]) + "\n")
                return
            else:
                with open(filename, mode) as f:
                    process = subprocess.Popen(command_part, stdout=f, preexec_fn=os.setpgrp)
                    if background:
                        jobs[job_counter] = Job(job_counter, process, " ".join(command_part))
                        print(f"[{job_counter}] {process.pid}")
                        job_counter += 1
                    else:
                        process.wait()
                return
        except (IndexError, ValueError):
            print("Syntax error in redirection")
            return
    try:
        if cmd_tokens[0] in builtins:
            builtins[cmd_tokens[0]](cmd_tokens)
        else:
            process = subprocess.Popen(cmd_tokens, preexec_fn=os.setpgrp)
            if background:
                jobs[job_counter] = Job(job_counter, process, " ".join(cmd_tokens))
                print(f"[{job_counter}] {process.pid}")
                job_counter += 1
            else:
                process.wait()
    except FileNotFoundError:
        print(f"{cmd_tokens[0]}: command not found")
    except Exception as e:
        print(f"Error executing command: {e}")

# ----------------- Main Loop ----------------- #
def shell_loop():
    while True:
        try:
            cmd = input("myshell> ")
            if not cmd.strip():
                continue
            tokens = cmd.split()
            background = False
            if tokens[-1] == '&':
                background = True
                tokens = tokens[:-1]
            execute_command(tokens, background)
        except KeyboardInterrupt:
            print()
        except EOFError:
            print("\nExiting shell.")
            break

if __name__ == "__main__":
    shell_loop()
