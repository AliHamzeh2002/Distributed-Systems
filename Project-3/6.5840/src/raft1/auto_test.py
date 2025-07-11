import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed

def run_test():
    result = subprocess.run(["go", "test", "-run", "3C"], capture_output=True, text=True)
    return result.stdout + result.stderr

def main():
    num_runs = 10
    with ThreadPoolExecutor(max_workers=num_runs) as executor:
        futures = [executor.submit(run_test) for _ in range(num_runs)]
        for i, future in enumerate(as_completed(futures), 1):
            print(f"--- Result {i} ---")
            print(future.result())

if __name__ == "__main__":
    main()
