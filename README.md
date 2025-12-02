# Hydro Optimize

## Installation

### ILP Solver
These crates rely on the [HiGHS](https://highs.dev/) ILP solver to find the optimal set of rewrites. The cmake compiler is required:

Linux:
```bash
sudo apt-get install cmake
```

MacOS (assuming HomeBrew is installed):
```bash
brew install cmake
```

### Profiling
MacOS profiling requires [samply](https://github.com/mstange/samply):
```bash
cargo install --locked samply
```