# Replicated Inventory Consistency Lab

A Python simulation of one primary replica, one secondary replica, and a client
that can read either replica. All writes go to the primary and are queued for
asynchronous replication to the secondary. The demo shows successful replication,
a stale read while replication is paused, and convergence after it resumes.

## Run

Requires Python 3.10 or newer. No external packages or services are needed.

```bash
git clone https://github.com/sagarparker/replication_consistency.git
cd replication_consistency
python3 demo.py
```

The demo exits successfully only if all scenario checks pass. Run the automated
tests separately with:

```bash
python3 -m unittest -v
```

## Architecture

```text
                       write(stock)
                  +--------------------+
                  |                    v
             +--------+           +---------+
             | Client | -- read -> | Primary |
             +--------+           +---------+
                  |                    |
                  | read               | enqueue update (stock, version)
                  v                    v
             +-----------+       +--------------------+
             | Secondary | <---- | FIFO queue + worker|
             +-----------+ async +--------------------+
                                  pause / resume
```

`inventory.py` keeps two independent immutable inventory snapshots in one process.
A background thread transfers queued updates in order, with a simulated 0.1-second
delay per update. A condition lock protects state; reads return immediately without
waiting for replication. `demo.py` acts as the client by calling `write(stock)` and
`read("primary")` or `read("secondary")`.

Pausing replication prevents the worker from applying updates while primary writes
and reads continue. Resuming lets the worker drain its backlog. Each write has an
increasing version, and the demo uses bounded waits for a specific version instead
of assuming replication has completed after a fixed sleep.

This is an in-memory simulation, not a network service. Recovery here means resuming
delayed replication with the queued updates intact; process crashes, persistent
storage, failover, and elections are outside the lab's scope. Closing the simulation
stops the worker and discards any pending in-memory updates.

## Results and evidence

The following is actual output from `python3 demo.py`. The complete captured log is
also in [`evidence/demo-output.txt`](evidence/demo-output.txt).

```text
REPLICATED INVENTORY LAB (Python, two in-memory replicas)

1. Successful asynchronous replication
  Client reads primary  : stock=10, version=0
  Client reads secondary: stock=10, version=0
  Client writes stock=12 to primary
  PASS: Primary immediately receives stock=12
  Client reads primary  : stock=12, version=1
  Client reads secondary: stock=12, version=1
  PASS: Both replicas return 12 after replication

2. Stale-read test
  Reset stock=10 through primary and wait for replication
  Client reads primary  : stock=10, version=2
  Client reads secondary: stock=10, version=2
  PASS: Both replicas start the stale-read test at 10
  Replication PAUSED; client writes stock=9 to primary
  Client reads primary  : stock=9, version=3
  Client reads secondary: stock=10, version=2
  PASS: Primary returns 9; secondary returns stale value 10
  PASS: Secondary remains behind during a 0.2-second replication pause

3. Recovery and convergence
  Replication RESUMED; wait for queued update
  Client reads primary  : stock=9, version=3
  Client reads secondary: stock=9, version=3
  PASS: Both replicas converge to 9 after replication resumes
  PASS: Replica versions also match

ALL SCENARIOS PASSED
```

The first scenario changes stock from 10 to 12 and verifies that both replicas
receive it. The second resets both replicas to 10, pauses replication, and writes 9
to the primary; the secondary still returns 10 throughout a bounded 0.2-second
wait. The third resumes replication and verifies that both stock and version match.

The four automated tests cover normal replication, stale reads and recovery,
multiple queued writes including repeated stock values, and invalid operations.
Their captured output is in [`evidence/test-output.txt`](evidence/test-output.txt).

## Reflection

Reading from a nearby secondary can reduce read latency and spread load across
replicas, but asynchronous replication means that replica may not yet have the
latest write. In this lab, the secondary quickly returns 10 even though the primary
has already accepted 9. Guaranteeing visibility of a completed write requires
reading the primary or waiting until a replica has applied at least that write's
version, which can increase latency and reduce availability during a delay. The
tradeoff is between serving a read immediately and waiting for evidence that the
replica is current enough.
