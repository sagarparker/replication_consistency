"""Run the three assignment scenarios and print verifiable evidence."""

from inventory import ReplicatedInventory


def show_reads(system: ReplicatedInventory) -> tuple[int, int]:
    values = []
    for name in ("primary", "secondary"):
        state = system.read(name)
        print(f"  Client reads {name:9s}: stock={state.stock}, version={state.version}")
        values.append(state.stock)
    return values[0], values[1]


def check(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)
    print(f"  PASS: {message}")


def main() -> None:
    print("REPLICATED INVENTORY LAB (Python, two in-memory replicas)")
    with ReplicatedInventory(stock=10) as system:
        print("\n1. Successful asynchronous replication")
        show_reads(system)
        print("  Client writes stock=12 to primary")
        write = system.write(12)
        check(system.read("primary") == write, "Primary immediately receives stock=12")
        print("  Reads immediately after write (informational; replication may still be pending)")
        show_reads(system)
        system.wait_for_replication(write.version)
        check(show_reads(system) == (12, 12), "Both replicas return 12 after replication")

        print("\n2. Stale-read test")
        print("  Reset stock=10 through primary and wait for replication")
        reset = system.write(10)
        system.wait_for_replication(reset.version)
        check(show_reads(system) == (10, 10), "Both replicas start the stale-read test at 10")
        system.pause_replication()
        print("  Replication PAUSED; client writes stock=9 to primary")
        write = system.write(9)
        check(show_reads(system) == (9, 10), "Primary returns 9; secondary returns stale value 10")
        try:
            system.wait_for_replication(write.version, timeout=0.2)
        except TimeoutError:
            print("  PASS: Secondary remains behind during a 0.2-second replication pause")
        else:
            raise AssertionError("Replication unexpectedly completed while paused")

        print("\n3. Recovery and convergence")
        print("  Replication RESUMED; wait for queued update")
        system.resume_replication()
        system.wait_for_replication(write.version)
        check(show_reads(system) == (9, 9), "Both replicas converge to 9 after replication resumes")
        check(system.read("primary") == system.read("secondary"), "Replica versions also match")
    print("\nALL SCENARIOS PASSED")


if __name__ == "__main__":
    main()
