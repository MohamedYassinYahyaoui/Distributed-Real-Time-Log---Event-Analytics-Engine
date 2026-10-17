import asyncio
import random
import time
import httpx
import os

# Ingestion Gateway endpoint URL
GATEWAY_URL = "http://localhost:8000/api/v1/events"

TOTAL_REQUESTS = int(os.getenv("TOTAL_REQUESTS", "50000"))
CONCURRENT_WORKERS = int(os.getenv("CONCURRENCY", "200"))

SERVICES = ["auth-service", "payment-service", "checkout-api", "inventory-db", "recommendation-engine"]
EVENT_TYPES = ["user_login", "payment_processed", "checkout_failed", "db_query", "item_viewed"]
ENVIRONMENTS = ["production", "staging"]

# Weighting event status types (80% normal latency, 20% high latency/errors)
def generate_payload():
    service = random.choice(SERVICES)
    event_type = random.choice(EVENT_TYPES)
    env = random.choice(ENVIRONMENTS)
    
    # Simulate realistic latency distributions
    if random.random() < 0.85:
        latency = random.randint(15, 120)
        payload_msg = "Operation completed successfully"
    else:
        latency = random.randint(450, 3500) # High latency spike
        payload_msg = "Operation timed out or thrown high latency warning"

    return {
        "service_name": service,
        "event_type": event_type,
        "environment": [env],
        "payload": payload_msg,
        "latency_ms": latency
    }

async def worker(client: httpx.AsyncClient, queue: asyncio.Queue, results: dict):
    while not queue.empty():
        _ = await queue.get()
        payload = generate_payload()
        try:
            start_time = time.perf_counter()
            response = await client.post(GATEWAY_URL, json=payload, timeout=5.0)
            elapsed = time.perf_counter() - start_time
            
            if response.status_code == 202:
                results["success"] += 1
            else:
                results["failed"] += 1
        except Exception:
            results["errors"] += 1
        finally:
            queue.task_done()

async def main():
    print(f"Starting load test: Pushing {TOTAL_REQUESTS} events using {CONCURRENT_WORKERS} concurrent workers...")
    
    queue = asyncio.Queue()
    for i in range(TOTAL_REQUESTS):
        queue.put_nowait(i)

    results = {"success": 0, "failed": 0, "errors": 0}
    start_time = time.perf_counter()

    async with httpx.AsyncClient(limits=httpx.Limits(max_connections=CONCURRENT_WORKERS)) as client:
        workers = [
            asyncio.create_task(worker(client, queue, results))
            for _ in range(CONCURRENT_WORKERS)
        ]
        await queue.join()
        for w in workers:
            w.cancel()

    total_time = time.perf_counter() - start_time
    rps = results["success"] / total_time if total_time > 0 else 0

    print("\n--- Load Test Results ---")
    print(f"Total Events Sent: {TOTAL_REQUESTS}")
    print(f"Successful (202):  {results['success']}")
    print(f"Failed (Non-202):  {results['failed']}")
    print(f"Network Errors:    {results['errors']}")
    print(f"Elapsed Time:      {total_time:.2f} seconds")
    print(f"Throughput:        {rps:.2f} req/sec")

if __name__ == "__main__":
    asyncio.run(main())