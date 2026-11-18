"""Behavior checks for replication, a paused backlog, and bounded waits."""

import unittest

from inventory import ReplicatedInventory


class ReplicationTests(unittest.TestCase):
    def test_primary_write_eventually_replicates(self):
        with ReplicatedInventory(replication_delay=0.01) as system:
            write = system.write(12)
            self.assertEqual(system.read("primary"), write)
            system.wait_for_replication(write.version)
            self.assertEqual(system.read("secondary"), write)

    def test_stale_read_and_recovery(self):
        with ReplicatedInventory(replication_delay=0.01) as system:
            system.pause_replication()
            write = system.write(9)
            self.assertEqual(system.read("primary").stock, 9)
            self.assertEqual(system.read("secondary").stock, 10)
            with self.assertRaises(TimeoutError):
                system.wait_for_replication(write.version, timeout=0.02)
            system.resume_replication()
            system.wait_for_replication(write.version)
            self.assertEqual(system.read("secondary"), write)

    def test_backlog_reaches_latest_version_even_with_repeated_values(self):
        with ReplicatedInventory(replication_delay=0.001) as system:
            system.pause_replication()
            for stock in (9, 8, 7, 10, 10):
                write = system.write(stock)
            self.assertEqual(system.read("secondary").version, 0)
            system.resume_replication()
            system.wait_for_replication(write.version)
            self.assertEqual(system.read("secondary"), write)
            self.assertEqual(write.version, 5)

    def test_invalid_input_and_write_after_shutdown(self):
        with ReplicatedInventory() as system:
            with self.assertRaises(ValueError):
                system.write(-1)
            with self.assertRaises(ValueError):
                system.read("unknown")
        with self.assertRaises(RuntimeError):
            system.write(9)


if __name__ == "__main__":
    unittest.main()
