# Hydro Optimize

Automatically apply decoupling and partitioning to Hydro programs for higher throughput.

> ![NOTE]
> Only Linux is supported, as we compile with `glibc` for better performance and more legibile `perf` results.


## Installation

### ILP Solver
We rely on the [Gurobi](https://www.gurobi.com/) ILP solver to find the optimal set of rewrites.
Gurobi is not free; you will need to either create a Gurobi account (and use a free educational license) or use your organization's license, if available.

```bash
./setup_gurobi.sh
```

Create a Gurobi license following your organization's instructions.
For free educational licenses, you may request a named license from Gurobi, then run the command they provide, which should resemble the following:

```bash
grbgetkey <unique-hash-from-gurobi>
```

## Execution

To run Microbus or Krupa without applying any optimizations, run:
```bash
cargo run --example benchmark_paxos -- --aws > paxos.txt
cargo run --example cas -- --aws > cas.txt
```
The current progress of each execution can be found in the piped `.txt` files.
Note that killing the process (or putting the running machine to sleep) will leave the machines hanging, so if you must kill the process, be sure to follow instructions in "Tearing Down" to tear it down.

### Automatic Optimization

Optimization relies on an accurate model of networking costs, which must first be calibrated with NetworkCalibrator.
This may take multiple hours, as it cycles through multiple network sizes, and gradually increases the number of clients for each size until throughput saturates.
```bash
cargo run --example network_calibrator -- --aws > network.txt
```

Once network calibration is complete, we can optimize either protocol by adding `--optimize`. For example:
```bash
cargo run --example benchmark_paxos -- --aws --optimize > paxos_optimize.txt
```

Run the command above continuously in a loop until it returns with no outputs.
Use the following script to automate that process:
```bash
./run_protocol.sh benchmark_paxos
./run_protocol.sh cas
```

All results are stored in `benchmark_results`.

## Tearing Down

If the experiment ever goes wrong, you can terminate it with `Ctrl+C`.
Then delete any cloud resources with:
```bash
./terraform_cleanup.sh
```

Afterwards, you may optionally delete the terraform files to speed up future terraform cleanups:
```bash
rm -rf .hydro
```
