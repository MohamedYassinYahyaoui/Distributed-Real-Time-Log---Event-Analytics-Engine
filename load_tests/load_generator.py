import asyncio
import json
import os
import random
import time
from multiprocessing import Pool

HOST = os.getenv("GATEWAY_HOST", "127.0.0.1")
PORT = int(os.getenv("GATEWAY_PORT", "8000"))
API_KEY = os.getenv("API_KEY", "sp_live_default_bench_key_2026")

TOTAL_REQUESTS = int(os.getenv("TOTAL_REQUESTS", "100000"))
NUM_PROCESSES = int(os.getenv("PROCESSES", "4"))
CONCURRENCY_PER_PROCESS = int(os.getenv("CONCURRENCY_PER_PROC", "25"))

SERVICES = ["auth-service", "payment-service", "checkout-api", "inventory-db", "recommendation-engine"]
EVENT_TYPES = ["user_login", "payment_processed", "checkout_failed", "db_query", "item_viewed"]
ENVIRONMENTS = ["production", "staging"]


def build_http_frames(pool_size: int = 200) -> list[bytes]:
    """Pre-builds raw HTTP/1.1 Keep-Alive POST frames (with X-API-Key auth) so 0% CPU is wasted on client overhead."""
    frames = []
    for _ in range(pool_size):
        latency = random.randint(8, 35) if random.random() < 0.90 else random.randint(45, 95)
        msg = "Operation completed successfully" if latency <= 35 else "High latency warning"
        body = json.dumps({
            "service_name": random.choice(SERVICES),
            "event_type": random.choice(EVENT_TYPES),
            "environment": [random.choice(ENVIRONMENTS)],
            "payload": msg,
            "latency_ms": latency,
        }).encode("utf-8")

        frame = (
            f"POST /api/v1/events HTTP/1.1\r\n"
            f"Host: {HOST}:{PORT}\r\n"
            f"X-API-Key: {API_KEY}\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"Connection: keep-alive\r\n\r\n"
        ).encode("ascii") + body
        frames.append(frame)
    return frames


async def socket_worker(num_requests: int, frames: list[bytes]) -> dict:
    results = {"success": 0, "failed": 0, "errors": 0}
    pool_len = len(frames)

    try:
        reader, writer = await asyncio.open_connection(HOST, PORT)
        for i in range(num_requests):
            try:
                writer.write(frames[i % pool_len])
                data = await reader.read(1024)
                if b"202 Accepted" in data:
                    results["success"] += 1
                else:
                    results["failed"] += 1
            except Exception:
                results["errors"] += 1
                reader, writer = await asyncio.open_connection(HOST, PORT)
        writer.close()
    except Exception:
        results["errors"] += num_requests

    return results


async def run_process_batch(requests_for_this_process: int) -> dict:
    frames = build_http_frames()
    base = requests_for_this_process // CONCURRENCY_PER_PROCESS
    rem = requests_for_this_process % CONCURRENCY_PER_PROCESS

    tasks = [
        socket_worker(base + (1 if i < rem else 0), frames)
        for i in range(CONCURRENCY_PER_PROCESS)
    ]
    worker_stats = await asyncio.gather(*tasks)

    totals = {"success": 0, "failed": 0, "errors": 0}
    for stat in worker_stats:
        for k in totals:
            totals[k] += stat[k]
    return totals


def process_entry(requests_for_this_process: int) -> dict:
    return asyncio.run(run_process_batch(requests_for_this_process))


if __name__ == "__main__":
    total_concurrency = NUM_PROCESSES * CONCURRENCY_PER_PROCESS
    print(
        f"Starting load test: {TOTAL_REQUESTS} events | "
        f"{NUM_PROCESSES} OS processes | {total_concurrency} concurrent keep-alive sockets..."
    )

    base = TOTAL_REQUESTS // NUM_PROCESSES
    rem = TOTAL_REQUESTS % NUM_PROCESSES
    work_splits = [base + (1 if i < rem else 0) for i in range(NUM_PROCESSES)]

    with Pool(processes=NUM_PROCESSES) as pool:
        pool.map(int, range(NUM_PROCESSES))  # Warm up worker processes
        start_time = time.perf_counter()
        proc_results = pool.map(process_entry, work_splits)
        total_time = time.perf_counter() - start_time

    final = {"success": 0, "failed": 0, "errors": 0}
    for r in proc_results:
        for k in final:
            final[k] += r[k]

    rps = final["success"] / total_time if total_time > 0 else 0

    print("\n--- Load Test Results ---")
    print(f"Total Events Sent: {TOTAL_REQUESTS}")
    print(f"Successful (202):  {final['success']}")
    print(f"Failed (Non-202):  {final['failed']}")
    print(f"Network Errors:    {final['errors']}")
    print(f"Elapsed Time:      {total_time:.2f} seconds")
    print(f"Throughput:        {rps:.2f} req/sec")
