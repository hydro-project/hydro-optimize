# Finding anomalies in predicted performance

## Installing Python

Install [uv](https://docs.astral.sh/uv/guides/install-python/) for managing the Python environment, then create a virtual environment and install the required packages:
```bash
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt
```

## Running the script

1. Run at least 2 iterations of an example with `deploy_and_analyze`. Route the output to a `.txt` file in this directory. For example:
   ```bash
   cargo run --example perf_paxos > perf_paxos.txt
   ```
2. Run the analysis script:
    ```bash
    source .venv/bin/activate
    uv run graph_anomalies.py
    ```
3. The graphs will be generated in this directory.