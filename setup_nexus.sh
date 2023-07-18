#!/bin/bash

module switch PrgEnv-nvhpc/ PrgEnv-gnu/

source /lus/grand/projects/datascience/cadams/NEXT/spack/share/spack/setup-env.sh
spack env activate nexus

NEXUS_INSTALL_DIR=/home/cadams/Polaris/NEXT/nexus-install
export PATH=${NEXUS_INSTALL_DIR}/bin/:$PATH
export LD_LIBRARY_PATH=${NEXUS_INSTALL_DIR}/lib/:$LD_LIBRARY_PATH
# export PATH=${NEXUS_INSTALL_DIR}:$PATH
