import subprocess
import time
import psutil

def monitor_ram(pid):
    try:
        proc = psutil.Process(pid)
        peak = 0
        while proc.is_running():
            mem = proc.memory_info().rss / (1024 * 1024)  # in MB
            if mem > peak:
                peak = mem
            time.sleep(0.1)
        return peak
    except psutil.NoSuchProcess:
        return peak

def run_and_monitor(script):
    print(f"\n[INFO] Running {script} ...")
    start = time.perf_counter()
    process = subprocess.Popen(["python", script])
    peak_mem = monitor_ram(process.pid)
    process.wait()
    end = time.perf_counter()
    duration = end - start
    print(f"[INFO] Peak RAM {script}: {peak_mem:.2f} MB")
    print(f"[INFO] Finished {script} in {duration:.2f} seconds\n")
    return duration, peak_mem

if __name__ == "__main__":
    total_time = 0
    peak_list = []

    for script in ["ELT/bronze.py", "ELT/silver.py", "ELT/gold.py"]:
        duration, peak = run_and_monitor(script)
        total_time += duration
        peak_list.append(peak)

    peak_ram_total = max(peak_list)

    print("\n[INFO] ELT Pipeline Summary:")
    print(f"[INFO] Total ELT Time    : {total_time:.2f} seconds")
    print(f"[INFO] Peak RAM (highest): {peak_ram_total:.2f} MB")
