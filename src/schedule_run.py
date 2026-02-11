import time
import extract_load
import datetime

# Interval in seconds (e.g., 600 = 10 minutes)
INTERVAL = 600

def schedule_loop():
    print(f"Starting scheduler loop. Pipeline will run every {INTERVAL} seconds.")
    
    while True:
        try:
            print(f"\n--- Triggering Pipeline at {datetime.datetime.now()} ---")
            extract_load.run_pipeline()
            print(f"--- Pipeline finished. Sleeping for {INTERVAL} seconds ---")
        except Exception as e:
            print(f"Wrapper caught exception: {e}")
        
        time.sleep(INTERVAL)

if __name__ == "__main__":
    schedule_loop()
