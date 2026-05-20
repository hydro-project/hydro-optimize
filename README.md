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
