import os
from datetime import datetime

class Logger:
    def __init__(self, log_dir="Log", console=True):
        self.log_dir = log_dir
        self.console = console
        os.makedirs(self.log_dir, exist_ok=True)

        self.current_date = None
        self.log_path = None
        self._update_log_file()

    def _update_log_file(self):
        today = datetime.now().strftime("%Y-%m-%d")
        if today != self.current_date:
            self.current_date = today
            self.log_path = os.path.join(
                self.log_dir, f"log_{today}.txt"
            )

    def _write(self, level, message):
        self._update_log_file()
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{timestamp}] [{level}] {message}\n"

        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(line)

        if self.console:
            print(line.strip())

    def info(self, msg): self._write("INFO", msg)
    def warning(self, msg): self._write("WARN", msg)
    def error(self, msg): self._write("ERROR", msg)
