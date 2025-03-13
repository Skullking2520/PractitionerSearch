# process_handler.py
import json
import signal
import sys
import time

class ProcessHandler:
    def __init__(self, progress_sheet, init_value, position, shutdown_callback=None):
        self.progress_sheet = progress_sheet
        self.position = position
        self.init_value = init_value
        self.shutdown_callback = shutdown_callback
        self.progress = self.load_progress()
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)

    def load_progress(self):
        retries = 10
        delay = 60
        for attempt in range(retries):
            try:
                progress_json = self.progress_sheet.acell(self.position).value
                if not progress_json:
                    progress = self.init_value
                else:
                    progress = json.loads(progress_json)
                return progress
            except Exception as e:
                print(f"Failed to load progress: {e}. Retrying in {delay} seconds... (Attempt {attempt + 1}/{retries})")
                time.sleep(delay)
                delay *= 2
        print("Failed to load progress after multiple attempts, finishing program")
        return {"progress": "finished"}

    def save_progress(self, progress):
        retries = 10
        delay = 60
        for attempt in range(retries):
            try:
                self.progress_sheet.update(self.position, [[json.dumps(progress)]])
                return
            except Exception as e:
                print(f"Failed to save progress: {e}. Retrying in {delay} seconds... (Attempt {attempt + 1}/{retries})")
                time.sleep(delay)
                delay *= 2
        print("Failed to save progress after multiple attempts.")

    def signal_handler(self, signum, frame):
        self.save_progress(self.progress)
        print(f"Signal {signum} occurred! Saving before shutdown...")
        if self.shutdown_callback:
            self.shutdown_callback()
        sys.exit(0)
