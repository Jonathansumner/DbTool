import signal
import threading

import humanize
from rich.console import Console
from rich.progress import (
    Progress, BarColumn, TextColumn, TimeElapsedColumn,
    TimeRemainingColumn, SpinnerColumn,
)
from rich.theme import Theme

theme = Theme({
    "info": "cyan",
    "success": "bold green",
    "warning": "bold yellow",
    "error": "bold red",
    "header": "bold magenta",
    "dim": "dim white",
})

console = Console(theme=theme)

# two-stage interrupt: first sets flag, second raises KeyboardInterrupt
interrupted = False
_interrupt_count = 0
_lock = threading.Lock()


def _signal_handler(sig, frame):
    global interrupted, _interrupt_count
    with _lock:
        _interrupt_count += 1
        if _interrupt_count == 1:
            interrupted = True
            console.print("\n[warning]⚠ interrupt — (press again to force quit)[/]")
        else:
            console.print("\n[error]force quit[/]")
            raise KeyboardInterrupt


def reset_interrupt():
    """reset interrupt state for new operations."""
    global interrupted, _interrupt_count
    with _lock:
        interrupted = False
        _interrupt_count = 0


signal.signal(signal.SIGINT, _signal_handler)


class ChunkProgress:
    def __init__(self, table_name: str, colour: str, total_rows: int, start_chunk: int, chunks_total: int, chunk_rows: int):
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn(f"[{colour}]" + "{task.fields[table]}"),
            BarColumn(bar_width=40),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("•"),
            TextColumn("{task.fields[rows_done]}/{task.fields[rows_total]} rows"),
            TextColumn("•"),
            TextColumn("{task.fields[chunk_info]}"),
            TextColumn("•"),
            TextColumn("{task.fields[speed]}"),
            TextColumn("•"),
            TimeElapsedColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
            console=console,
            transient=False,
        )
        self.table_name = table_name
        self.total_rows = total_rows
        self.start_chunk = start_chunk
        self.chunks_total = chunks_total
        self.chunk_rows = chunk_rows

    def __enter__(self):
        self.progress.__enter__()
        self.task = self.progress.add_task(
            "Processing",
            total=self.total_rows,
            completed=self.start_chunk * self.chunk_rows,
            table=self.table_name,
            rows_done=humanize.intcomma(self.start_chunk * self.chunk_rows),
            rows_total=humanize.intcomma(self.total_rows),
            chunk_info=f"chunk {self.start_chunk}/{self.chunks_total}",
            speed="",
        )
        return self

    def __exit__(self, *args):
        return self.progress.__exit__(*args)

    def update(self, rows_done: int, chunk_idx: int, speed_str: str):
        self.progress.update(
            self.task, completed=rows_done,
            rows_done=humanize.intcomma(rows_done),
            chunk_info=f"chunk {chunk_idx}/{self.chunks_total}",
            speed=speed_str,
        )