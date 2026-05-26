#!/usr/bin/env bash
set -euo pipefail

cd /opt
sudo wget https://packages.gurobi.com/13.0/gurobi13.0.2_linux64.tar.gz
sudo tar xvfz gurobi13.0.2_linux64.tar.gz
echo 'export GUROBI_HOME="/opt/gurobi1302/linux64"' >> ~/.zshrc
echo 'export PATH="${PATH}:${GUROBI_HOME}/bin"' >> ~/.zshrc
echo 'export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${GUROBI_HOME}/lib"' >> ~/.zshrc
source ~/.zshrc
