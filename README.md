# Hydro Optimize

Automatically apply decoupling and partitioning to Hydro programs for higher throughput.

## Installation

### Rust
Install Rust. Install `rust-analyzer` as well if you plan on editing the code in an IDE.
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.bashrc
rustup target add x86_64-unknown-linux-musl

# Optional
rustup component add rust-analyzer
```

### Permissions
The Linux machine on which you are running hydro-optimize will need permissions to launch VMs. If this is your local machine, you can simply sign into your AWS account locally; otherwise you will need to grant the remote machine the appropriate permissions.

#### AWS EC2
1. Go to AWS IAM Roles > Create role.
2. Select "EC2" as the Use case.
3. Add "AmazonEC2FullAccess" as the Permission policy.
4. Give it a name (I named it "EC2FullRole"). 
5. Go to the Instance you are running hydro-optimize from, Actions > Security > Modify IAM role.
6. Give it the EC2FullRole.

#### GCP Compute Engine
TODO

### Terraform
Terraform is used to spin up machines.
These are instructions for Amazon Linux machines; refer to [the official site](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli) for your architecture.
```bash
sudo dnf install -y dnf-plugins-core
sudo dnf config-manager --add-repo https://rpm.releases.hashicorp.com/AmazonLinux/hashicorp.repo
sudo dnf -y install terraform
```

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
