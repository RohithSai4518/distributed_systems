.PHONY: all install build run test chaos bench clean

all: build test

install:
	@echo "Installing Aegis Distributed Systems Engine..."
	python -m pip install -e .

build:
	@echo "Compiling bytecode and verifying assets..."
	python -m compileall aegis/ web/

run:
	@echo "Starting 3-Node Distributed Cluster & Dashboard on http://localhost:8080..."
	python main.py cluster --nodes 3 --http-port 8080

test:
	@echo "Running full test suite..."
	python main.py test

chaos:
	@echo "Executing chaos engineering and partition failover suite..."
	python main.py chaos

bench:
	@echo "Running load test and latency percentile micro-benchmarks..."
	python main.py benchmark --workers 4 --ops-per-worker 100

clean:
	@echo "Cleaning runtime data..."
	rm -rf data data_chaos .pytest_cache
