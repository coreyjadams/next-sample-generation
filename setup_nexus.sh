#!/bin/bash

source /home/cadams/spack/share/spack/setup-env.sh
spack env activate nexus

NEXUS_INSTALL_DIR=/home/cadams/NEXT/nexus-install
export PATH=${NEXUS_INSTALL_DIR}/bin/:$PATH
export LD_LIBRARY_PATH=${NEXUS_INSTALL_DIR}/lib/:$LD_LIBRARY_PATH
# export PATH=${NEXUS_INSTALL_DIR}:$PATH