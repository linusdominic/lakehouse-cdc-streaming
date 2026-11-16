"""Command line entry point: `uv run cdc --help`."""

from __future__ import annotations

import json
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import click

from lakehouse_cdc.config import ARTIFACTS_DIR, REPO_ROOT, load_settings
from lakehouse_cdc.contracts import PAYMENTS, topic_for

CONNECTOR_NAME = "loans-cdc"
CONNECTOR_TEMPLATE = REPO_ROOT / "docker" / "connect" / "loans-cdc.json"


@click.group()
def main() -> None:
    """Tools for driving and checking the CDC lakehouse."""


@main.command()
@click.option("--borrowers", default=200, show_default=True)
@click.option("--seed", default=42, show_default=True)
def seed(borrowers: int, seed: int) -> None:
    """Insert borrowers before the connector exists, so the snapshot has rows to read."""
    from lakehouse_cdc.clients import pg_connect
    from lakehouse_cdc.generator import executor, planner

    settings = load_settings()
    with pg_connect(settings) as conn:
        if conn.execute("SELECT count(*) FROM borrowers").fetchone()[0]:
            click.echo("borrowers already present, skipping seed")
            return
        executor.execute(conn, planner.plan_seed(borrowers, seed), progress_every=0)
    click.echo(f"seeded {borrowers} borrowers")


@main.command("register-connector")
@click.option("--timeout", default=120, show_default=True)
def register_connector(timeout: int) -> None:
    """Create or update the Debezium connector and wait until it is RUNNING."""
    settings = load_settings()
    raw = CONNECTOR_TEMPLATE.read_text()
    config = re.sub(r"\$\{(\w+)\}", lambda m: settings.env[m.group(1)], raw)
    url = f"{settings.connect_url}/connectors/{CONNECTOR_NAME}"
    request = urllib.request.Request(
        f"{url}/config",
        data=config.encode(),
        method="PUT",
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(request, timeout=30).read()
    deadline = time.monotonic() + timeout
    state = "UNKNOWN"
    while time.monotonic() < deadline:
        try:
            status = json.loads(urllib.request.urlopen(f"{url}/status", timeout=10).read())
            tasks = status.get("tasks", [])
            state = status["connector"]["state"] + "/" + ",".join(t["state"] for t in tasks)
            if any(t["state"] == "FAILED" for t in tasks):
                click.echo(json.dumps(status, indent=2))
                sys.exit(1)
            if (
                status["connector"]["state"] == "RUNNING"
                and tasks
                and all(t["state"] == "RUNNING" for t in tasks)
            ):
                click.echo(f"connector {CONNECTOR_NAME} is RUNNING")
                return
        except urllib.error.URLError:
            pass
        time.sleep(2)
    click.echo(f"connector did not reach RUNNING within {timeout}s (state {state})")
    sys.exit(1)


@main.command()
@click.option("--seed", default=42, show_default=True)
@click.option("--ops", default=20000, show_default=True, help="Number of transactions.")
@click.option("--rate", default=250.0, show_default=True, help="Transactions per second cap.")
@click.option(
    "--schema-change/--no-schema-change",
    default=True,
    show_default=True,
    help="ALTER TABLE payments ADD COLUMN channel halfway through.",
)
@click.option(
    "--replay-every",
    default=2500,
    show_default=True,
    help="Replay recent Kafka records every N transactions (0 = never).",
)
@click.option(
    "--replay-size",
    default=150,
    show_default=True,
    help="Records replayed per partition each time.",
)
@click.option(
    "--poison-every",
    default=3000,
    show_default=True,
    help="Publish a poison record every N transactions (0 = never).",
)
@click.option("--label", default="run", show_default=True, help="Name of the manifest file.")
def generate(seed, ops, rate, schema_change, replay_every, replay_size, poison_every, label) -> None:  # fmt: skip
    """Run the OLTP workload against Postgres, with fault injection."""
    from lakehouse_cdc.clients import pg_connect
    from lakehouse_cdc.contracts import CONTRACTS
    from lakehouse_cdc.generator import executor, planner
    from lakehouse_cdc.inject import Injector

    settings = load_settings()
    injector = Injector(settings.kafka_bootstrap, seed)
    injected = {"replayed_records": 0, "poison_records": 0}
    replay_topics = [topic_for(t) for t in CONTRACTS]

    def do_replay() -> None:
        for topic in replay_topics:
            injected["replayed_records"] += injector.replay_recent(topic, replay_size)

    def do_poison() -> None:
        injected["poison_records"] += injector.poison(topic_for(PAYMENTS.source_table))

    hooks = {}
    if replay_every:
        hooks.update(dict.fromkeys(range(replay_every, ops, replay_every), do_replay))
    if poison_every:
        # Offset by one so a poison hook never lands on a replay hook.
        hooks.update({i + 1: do_poison for i in range(poison_every, ops - 1, poison_every)})

    with pg_connect(settings) as conn:
        already = executor.channel_exists(conn)
        cfg = planner.PlanConfig(
            seed=seed,
            n_ops=ops,
            id_base=executor.current_id_base(conn),
            existing_borrowers=executor.existing_borrowers(conn),
            schema_change_at=ops // 2 if schema_change and not already else None,
            channel_exists=already,
        )
        click.echo(
            f"workload: seed={seed} transactions={ops} rate<={rate:.0f}/s id_base={cfg.id_base} "
            f"schema_change_at={cfg.schema_change_at}"
        )
        started = time.time()
        counts = executor.execute(conn, planner.plan_workload(cfg), rate=rate, hooks=hooks)
        elapsed = time.time() - started

    manifest = {
        "seed": seed,
        "transactions": ops,
        "elapsed_seconds": round(elapsed, 1),
        "transactions_per_second": round(ops / elapsed, 1),
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(started)),
        "schema_change_at": cfg.schema_change_at,
        "by_kind": dict(sorted(counts.items())),
        **injected,
    }
    ARTIFACTS_DIR.mkdir(exist_ok=True)
    (ARTIFACTS_DIR / f"manifest-{label}.json").write_text(json.dumps(manifest, indent=2) + "\n")
    click.echo(json.dumps(manifest, indent=2))


@main.command()
@click.argument("topic")
@click.option("--partition", type=int, default=None, help="Default: all partitions.")
@click.option("--from-offset", type=int, default=None)
@click.option("--to-offset", type=int, default=None, help="Exclusive.")
@click.option("--last", type=int, default=None, help="Replay the newest N records per partition.")
@click.option("--seed", default=0, show_default=True)
def replay(topic, partition, from_offset, to_offset, last, seed) -> None:
    """Re-publish a Kafka offset range (shuffled) to simulate duplicate, out-of-order delivery."""
    from lakehouse_cdc.inject import Injector

    injector = Injector(load_settings().kafka_bootstrap, seed)
    if last is not None:
        n = injector.replay_recent(topic, last)
    else:
        if from_offset is None or to_offset is None:
            raise click.UsageError("give --last, or both --from-offset and --to-offset")
        parts = [partition] if partition is not None else injector.partitions(topic)
        n = sum(injector.replay_range(topic, p, from_offset, to_offset) for p in parts)
    click.echo(f"replayed {n} records to {topic}")


@main.command()
@click.option("--timeout", default=600, show_default=True)
def drain(timeout: int) -> None:
    """Wait until every commit made in Postgres so far is in silver."""
    from lakehouse_cdc.drain import DrainTimeout
    from lakehouse_cdc.drain import drain as run_drain

    try:
        waited = run_drain(load_settings(), timeout_s=timeout)
    except DrainTimeout as exc:
        click.echo(f"DRAIN FAILED: {exc}")
        sys.exit(1)
    click.echo(f"pipeline drained in {waited:.1f}s")


@main.command()
def verify() -> None:
    """Reconcile Postgres against the lake. Exit code 1 on any mismatch."""
    from lakehouse_cdc.verify import format_report, run_checks

    checks = run_checks(load_settings())
    click.echo(format_report(checks))
    sys.exit(0 if all(c.passed for c in checks) else 1)


@main.command("sample-lag")
@click.option("--out", type=click.Path(path_type=Path), default=ARTIFACTS_DIR / "lag.csv")
@click.option("--interval", default=3.0, show_default=True)
@click.option("--duration", type=float, default=None, help="Seconds; default until signalled.")
def sample_lag(out: Path, interval: float, duration: float | None) -> None:
    """Sample consumer lag to a CSV until stopped."""
    from lakehouse_cdc.observe import sample_lag as run

    n = run(load_settings(), out, interval, duration)
    click.echo(f"wrote {n} lag samples to {out}")


@main.command()
@click.option("--lag-csv", type=click.Path(path_type=Path), default=ARTIFACTS_DIR / "lag.csv")
@click.option("--since", default=None, help="ISO-8601 timestamp; only count events after it.")
def report(lag_csv: Path, since: str | None) -> None:
    """Print consumer lag, freshness and micro-batch statistics."""
    from lakehouse_cdc.observe import format_report

    click.echo(format_report(load_settings(), lag_csv, since))


@main.command("last-batch")
def last_batch() -> None:
    """Show what the newest micro-batch left in bronze and whether it completed."""
    from lakehouse_cdc.clients import Lake
    from lakehouse_cdc.observe import last_batch_state

    click.echo(last_batch_state(Lake(load_settings())))


if __name__ == "__main__":
    main()
