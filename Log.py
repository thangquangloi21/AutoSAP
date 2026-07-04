import os
from datetime import datetime


class Logger:
    def __init__(self, log_dir="Logs", console=True):
        self.log_dir = log_dir
        self.console = console
        self.current_date = None
        self.log_path = None
        self._update_log_file()

    def _update_log_file(self):
        now = datetime.now()
        today = now.strftime("%Y-%m-%d")
        if today != self.current_date:
            self.current_date = today
            log_dir = os.path.join(self.log_dir, now.strftime("%Y"), now.strftime("%m"))
            os.makedirs(log_dir, exist_ok=True)
            self.log_path = os.path.join(log_dir, f"log_{today}.txt")

    def _write(self, level, message):
        self._update_log_file()
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{timestamp}] [{level}] {message}"

        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")

        if self.console:
            print(line)

    def info(self, msg, *args):
        if args:
            msg = msg % args
        self._write("INFO", str(msg))

    def warning(self, msg, *args):
        if args:
            msg = msg % args
        self._write("WARN", str(msg))

    def error(self, msg, *args):
        if args:
            msg = msg % args
        self._write("ERROR", str(msg))
