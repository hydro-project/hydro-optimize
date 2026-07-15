# Scripts for generating graphs

## Installing Python

Install [uv](https://docs.astral.sh/uv/guides/install-python/) for managing the Python environment, then create a virtual environment and install the required packages:
```bash
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt
```

## Generating Encrypt exhaustive optimizations

Before running exhaustive search for Encrypt, generate:

- `benchmark_results/Encrypt_optimization_state.json`
- `benchmark_results/Encrypt_default_none/id.txt`

Then run:

```bash
python3 scripts/generate_encrypt_exhaustive_optimizations.py
```

This writes:

```text
benchmark_results/Encrypt_exhaustive_optimizations.json
```

To use the generated file:

```bash
cargo run --example encrypt -- --search
```

The generator omits the default colocated deployment where all operators are assigned to the same unpartitioned location.
