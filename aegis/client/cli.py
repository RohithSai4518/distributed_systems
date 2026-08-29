"""
Interactive Aegis Cluster CLI & REPL.
Supports interactive cluster management, KV operations, and chaos fault injection.
"""

import cmd
import shlex
import sys
from typing import List, Tuple

from aegis.client.sdk import AegisClient


class AegisCLI(cmd.Cmd):
    intro = """
======================================================================
       AEGIS DISTRIBUTED FAULT-TOLERANT KEY-VALUE CLUSTER CLI
======================================================================
Type 'help' or '?' to list commands. Type 'quit' to exit.
"""
    prompt = "aegis> "

    def __init__(self, seeds: List[Tuple[str, int]]):
        super().__init__()
        self.seeds = seeds
        self.client = AegisClient(seed_nodes=seeds)

    def do_put(self, arg):
        """put <key> <value> : Stores a key-value pair across the cluster."""
        parts = shlex.split(arg)
        if len(parts) < 2:
            print("Usage: put <key> <value>")
            return
        key, val = parts[0], parts[1]
        ok = self.client.put(key, val)
        if ok:
            print(f"[SUCCESS] Stored: '{key}' => '{val}'")
        else:
            print(f"[FAILURE] Could not store key '{key}'")

    def do_get(self, arg):
        """get <key> : Retrieves the value for a key."""
        parts = shlex.split(arg)
        if len(parts) < 1:
            print("Usage: get <key>")
            return
        key = parts[0]
        found, val = self.client.get(key)
        if found:
            print(f"[FOUND] {key} = {val}")
        else:
            print(f"[NOT FOUND] Key '{key}' does not exist in cluster.")

    def do_delete(self, arg):
        """delete <key> : Deletes a key."""
        parts = shlex.split(arg)
        if len(parts) < 1:
            print("Usage: delete <key>")
            return
        key = parts[0]
        ok = self.client.delete(key)
        if ok:
            print(f"[SUCCESS] Deleted key '{key}'")
        else:
            print(f"[FAILURE] Could not delete key '{key}'")

    def do_cas(self, arg):
        """cas <key> <prev_value> <new_value> : Atomic Compare-And-Swap."""
        parts = shlex.split(arg)
        if len(parts) < 3:
            print("Usage: cas <key> <prev_value> <new_value>")
            return
        key, prev_v, new_v = parts[0], parts[1], parts[2]
        ok, res = self.client.cas(key, prev_v, new_v)
        if ok:
            print(f"[SUCCESS] CAS succeeded! '{key}' is now '{res}'")
        else:
            print(f"[FAILED] CAS failed! Current value is: '{res}'")

    def do_scan(self, arg):
        """scan [start_key] [limit] : Scans keys in lexicographical order."""
        parts = shlex.split(arg)
        start_k = parts[0] if len(parts) > 0 else ""
        limit = int(parts[1]) if len(parts) > 1 else 50
        items = self.client.scan(start_k, limit)
        print(f"--- Scan Results ({len(items)} items) ---")
        for k, v in items:
            print(f"  {k} => {v}")

    def do_quit(self, arg):
        """quit : Exits the CLI."""
        self.client.close()
        print("Goodbye.")
        return True

    def do_exit(self, arg):
        """exit : Exits the CLI."""
        return self.do_quit(arg)


def main():
    seeds = [("127.0.0.1", 9001), ("127.0.0.1", 9002), ("127.0.0.1", 9003)]
    cli = AegisCLI(seeds)
    try:
        cli.cmdloop()
    except KeyboardInterrupt:
        print("\nExiting...")


if __name__ == "__main__":
    main()
