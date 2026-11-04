import requests, time, statistics

def run_test():
    lags = []
    print("Sending 20 requests to measure eventual consistency lag...")
    
    for i in range(20):
        # 1. Start clock & submit
        start_time = time.time()
        res = requests.post("http://localhost:8082/documents", json={"text": f"Test {i}"})
        doc_id = res.json()["id"]
        
        # 2. Poll the status endpoint until the saga completes
        while True:
            status_res = requests.get(f"http://localhost:8082/documents/{doc_id}/status")
            if status_res.json()["status"] == "COMPLETED":
                end_time = time.time()
                lag_ms = (end_time - start_time) * 1000
                lags.append(lag_ms)
                print(f"Request {i+1}/20: Completed in {lag_ms:.2f} ms")
                break
            time.sleep(0.05) # Poll every 50ms
            
    print("\n--- TEST 3 RESULTS ---")
    print(f"Min Lag:    {min(lags):.2f} ms")
    print(f"Median Lag: {statistics.median(lags):.2f} ms")
    print(f"Max Lag:    {max(lags):.2f} ms")

if __name__ == "__main__":
    run_test()