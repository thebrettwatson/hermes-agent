"""Two-process OS-level SessionDB turn lease contention (#84234 residual)."""

from __future__ import annotations

import multiprocessing
import os
from pathlib import Path

from hermes_state import SessionDB

_CONVERSATION_ID = "gemini-route-conv"


def _holder_worker(
    db_path: str,
    ready: "multiprocessing.synchronize.Event",
    contended: "multiprocessing.synchronize.Event",
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
    if not contended.wait(10):
        results.put(("holder_contended_timeout",))
        return
    db.release_session_turn_lease(_CONVERSATION_ID, holder)
    results.put(("holder_released", holder))
    released.set()


def _waiter_worker(
    db_path: str,
    start: "multiprocessing.synchronize.Event",
    contended: "multiprocessing.synchronize.Event",
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

    contended.set()
    results.put(("waiter_blocked",))

    acquired = db.acquire_session_turn_lease(
        _CONVERSATION_ID,
        holder,
        ttl_seconds=5,
        wait_seconds=2,
        poll_interval_seconds=0.02,
    )
    if acquired:
        results.put(("waiter_acquired",))
        db.release_session_turn_lease(_CONVERSATION_ID, holder)
    else:
        results.put(("waiter_acquire_failed",))


def test_turn_lease_serializes_across_two_os_processes(tmp_path: Path) -> None:
    """Exactly one process holds the lease; the second waits until release."""
    db_file = tmp_path / "state.db"
    db_path = str(db_file)
    parent = SessionDB(db_file)
    parent.create_session(_CONVERSATION_ID, source="test")

    ctx = multiprocessing.get_context("spawn")
    ready = ctx.Event()
    contended = ctx.Event()
    released = ctx.Event()
    start = ctx.Event()
    holder_results: "multiprocessing.queues.Queue[tuple[str, ...]]" = ctx.Queue()
    waiter_results: "multiprocessing.queues.Queue[tuple[str, ...]]" = ctx.Queue()

    holder = ctx.Process(
        target=_holder_worker,
        args=(db_path, ready, contended, released, holder_results),
    )
    waiter = ctx.Process(
        target=_waiter_worker,
        args=(db_path, start, contended, waiter_results),
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

    holder_acquired = holder_results.get(timeout=5)
    holder_released = holder_results.get(timeout=5)
    waiter_blocked = waiter_results.get(timeout=5)
    waiter_acquired = waiter_results.get(timeout=5)

    assert holder_acquired[0] == "holder_acquired"
    assert holder_released[0] == "holder_released"
    assert waiter_blocked[0] == "waiter_blocked"
    assert waiter_acquired[0] == "waiter_acquired"
