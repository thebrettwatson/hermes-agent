"""Two-process OS-level SessionDB turn lease contention (#84234 residual)."""

from __future__ import annotations

import multiprocessing
import os
import time
from pathlib import Path

from hermes_state import SessionDB

_CONVERSATION_ID = "gemini-route-conv"


def _holder_worker(
    db_path: str,
    ready: "multiprocessing.synchronize.Event",
    released: "multiprocessing.synchronize.Event",
    results: "multiprocessing.queues.Queue[tuple[str, ...]]",
) -> None:
    db = SessionDB(Path(db_path))
    holder = f"pid={os.getpid()}:turn=holder"
    if not db.try_acquire_session_turn_lease(
        _CONVERSATION_ID, holder, ttl_seconds=5
    ):
        results.put(("holder_acquire_failed",))
        return
    ready.set()
    results.put(("holder_acquired", holder))
    time.sleep(0.4)
    db.release_session_turn_lease(_CONVERSATION_ID, holder)
    results.put(("holder_released", holder))
    released.set()


def _waiter_worker(
    db_path: str,
    start: "multiprocessing.synchronize.Event",
    results: "multiprocessing.queues.Queue[tuple[str, ...]]",
) -> None:
    if not start.wait(10):
        results.put(("waiter_timeout",))
        return

    db = SessionDB(Path(db_path))
    holder = f"pid={os.getpid()}:turn=waiter"
    if db.try_acquire_session_turn_lease(_CONVERSATION_ID, holder, ttl_seconds=5):
        results.put(("waiter_early_acquire",))
        db.release_session_turn_lease(_CONVERSATION_ID, holder)
        return

    started = time.monotonic()
    acquired = db.acquire_session_turn_lease(
        _CONVERSATION_ID,
        holder,
        ttl_seconds=5,
        wait_seconds=2,
        poll_interval_seconds=0.02,
    )
    elapsed = time.monotonic() - started
    results.put(("waiter_acquired", str(acquired), f"{elapsed:.3f}"))
    if acquired:
        db.release_session_turn_lease(_CONVERSATION_ID, holder)


def _drain_queue(queue: "multiprocessing.queues.Queue[tuple[str, ...]]") -> list[tuple[str, ...]]:
    items: list[tuple[str, ...]] = []
    while not queue.empty():
        items.append(queue.get_nowait())
    return items


def test_turn_lease_serializes_across_two_os_processes(tmp_path: Path) -> None:
    """Exactly one process holds the lease; the second waits until release."""
    db_file = tmp_path / "state.db"
    db_path = str(db_file)
    parent = SessionDB(db_file)
    parent.create_session(_CONVERSATION_ID, source="test")

    ctx = multiprocessing.get_context("spawn")
    ready = ctx.Event()
    released = ctx.Event()
    start = ctx.Event()
    holder_results: "multiprocessing.queues.Queue[tuple[str, ...]]" = ctx.Queue()
    waiter_results: "multiprocessing.queues.Queue[tuple[str, ...]]" = ctx.Queue()

    holder = ctx.Process(
        target=_holder_worker,
        args=(db_path, ready, released, holder_results),
    )
    waiter = ctx.Process(
        target=_waiter_worker,
        args=(db_path, start, waiter_results),
    )

    holder.start()
    assert ready.wait(5), "holder process did not acquire the lease"
    start.set()
    waiter.start()

    holder.join(timeout=5)
    waiter.join(timeout=5)
    assert holder.exitcode == 0
    assert waiter.exitcode == 0
    assert released.is_set()

    holder_msgs = _drain_queue(holder_results)
    waiter_msgs = _drain_queue(waiter_results)

    assert ("holder_acquired",) == tuple(holder_msgs[0][:1])
    assert ("holder_released",) == tuple(holder_msgs[1][:1])
    assert not any(msg[0] == "waiter_early_acquire" for msg in waiter_msgs)

    acquired_msgs = [msg for msg in waiter_msgs if msg[0] == "waiter_acquired"]
    assert len(acquired_msgs) == 1
    assert acquired_msgs[0][1] == "True"
    assert float(acquired_msgs[0][2]) >= 0.15
