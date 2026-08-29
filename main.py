#!/usr/bin/env python3
"""
Aegis Distributed Systems Core - Master Entrypoint.
Commands:
  cluster   - Launch an N-node distributed cluster with HTTP gateway & visualizer
  node      - Launch an individual node server
  cli       - Start interactive cluster REPL
  benchmark - Run high-throughput concurrency load tests
  chaos     - Execute automated Jepsen-style chaos fault injection tests
  test      - Run comprehensive automated test suite
"""

import argparse
import os
import sys
import time
import unittest

from aegis.benchmarks.load_tester import LoadTester
from aegis.chaos.simulator import ChaosHarness
from aegis.client.cli import AegisCLI
from aegis.common.logger import Logger
from aegis.server.cluster import AegisCluster
from aegis.server.http_gateway import HTTPGatewayServer
from aegis.server.node import AegisNode


def cmd_cluster(args):
    """Spawns an in-process multi-node cluster with web dashboard."""
    logger = Logger(node_id="MAIN")
    logger.info("Initializing Aegis %d-Node Distributed Cluster...", args.nodes)

    cluster = AegisCluster(
        node_count=args.nodes,
        base_port=args.base_port,
        base_http_port=args.base_http_port,
        data_dir=args.data_dir
    )
    cluster.setup()
    cluster.start()

    # Launch HTTP Gateway & Visualizer
    gateway = HTTPGatewayServer(
        host=args.http_host,
        port=args.http_port,
        cluster=cluster,
        web_dir=args.web_dir
    )
    gateway.start()

    logger.info("========================================================================")
    logger.info(" Aegis Distributed Cluster is LIVE & HEALTHY!")
    logger.info(" Web Visualizer Dashboard: http://localhost:%d", args.http_port)
    logger.info(" TCP Node Cluster Ports:   %d - %d", args.base_port, args.base_port + args.nodes - 1)
    logger.info(" Press Ctrl+C to gracefully stop the cluster.")
    logger.info("========================================================================")

    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        logger.info("Shutting down cluster...")
        gateway.stop()
        cluster.stop()
        logger.info("Cluster shutdown complete.")


def cmd_cli(args):
    """Starts interactive cluster CLI."""
    seeds = [(args.host, p) for p in range(args.base_port, args.base_port + args.nodes)]
    cli = AegisCLI(seeds=seeds)
    cli.cmdloop()


def cmd_benchmark(args):
    """Runs throughput and latency benchmark."""
    seeds = [(args.host, p) for p in range(args.base_port, args.base_port + args.nodes)]
    tester = LoadTester(
        seed_nodes=seeds,
        num_workers=args.workers,
        ops_per_worker=args.ops_per_worker
    )
    result = tester.run_benchmark(read_ratio=args.read_ratio)
    result.print_report()


def cmd_chaos(args):
    """Runs automated chaos experiments."""
    logger = Logger(node_id="CHAOS-RUNNER")
    logger.info("Starting automated chaos suite...")

    cluster = AegisCluster(node_count=3, base_port=9300, base_http_port=8300, data_dir="./data_chaos")
    cluster.setup()
    cluster.start()

    harness = ChaosHarness(cluster)
    ok1 = harness.test_leader_failover()
    ok2 = harness.test_network_partition()

    cluster.stop()

    if ok1 and ok2:
        logger.info("ALL CHAOS TESTS PASSED WITH ZERO DATA LOSS!")
    else:
        logger.error("Chaos tests encountered failures!")
        sys.exit(1)


def cmd_test(args):
    """Executes unit & integration test suites."""
    loader = unittest.TestLoader()
    suite = loader.discover("tests", pattern="test_*.py")
    runner = unittest.TextTestRunner(verbosity=2)
    res = runner.run(suite)
    if not res.wasSuccessful():
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Aegis Distributed Systems Engine")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Cluster Subcommand
    p_cluster = subparsers.add_parser("cluster", help="Start multi-node cluster")
    p_cluster.add_argument("--nodes", type=int, default=3, help="Number of cluster nodes")
    p_cluster.add_argument("--base-port", type=int, default=9001, help="Base TCP port")
    p_cluster.add_argument("--base-http-port", type=int, default=8001, help="Base internal HTTP port")
    p_cluster.add_argument("--http-host", type=str, default="0.0.0.0", help="Dashboard host")
    p_cluster.add_argument("--http-port", type=int, default=8080, help="Dashboard port")
    p_cluster.add_argument("--data-dir", type=str, default="./data", help="Data root directory")
    p_cluster.add_argument("--web-dir", type=str, default="./web", help="Web directory")
    p_cluster.set_defaults(func=cmd_cluster)

    # CLI Subcommand
    p_cli = subparsers.add_parser("cli", help="Interactive REPL CLI")
    p_cli.add_argument("--host", type=str, default="127.0.0.1")
    p_cli.add_argument("--base-port", type=int, default=9001)
    p_cli.add_argument("--nodes", type=int, default=3)
    p_cli.set_defaults(func=cmd_cli)

    # Benchmark Subcommand
    p_bench = subparsers.add_parser("benchmark", help="Run performance load tests")
    p_bench.add_argument("--host", type=str, default="127.0.0.1")
    p_bench.add_argument("--base-port", type=int, default=9001)
    p_bench.add_argument("--nodes", type=int, default=3)
    p_bench.add_argument("--workers", type=int, default=8, help="Concurrent client workers")
    p_bench.add_argument("--ops-per-worker", type=int, default=200, help="Operations per worker")
    p_bench.add_argument("--read-ratio", type=float, default=0.5, help="Ratio of GET operations (0.0 to 1.0)")
    p_bench.set_defaults(func=cmd_benchmark)

    # Chaos Subcommand
    p_chaos = subparsers.add_parser("chaos", help="Execute automated chaos tests")
    p_chaos.set_defaults(func=cmd_chaos)

    # Test Subcommand
    p_test = subparsers.add_parser("test", help="Run automated test suite")
    p_test.set_defaults(func=cmd_test)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
