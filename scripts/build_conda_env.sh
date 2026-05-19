#!/usr/bin/env bash
# Build a conda-pack archive of the Python env used by spark_job.py.
# This is the standard way to ship Python dependencies to YARN/cluster nodes.

set -euo pipefail

ENV_NAME="hw3-env"

conda create -n "${ENV_NAME}" -c conda-forge -y \
    python=3.10 pyspark=3.5 pyarrow pandas

conda install -n "${ENV_NAME}" -c conda-forge -y conda-pack
conda run -n "${ENV_NAME}" conda-pack -o hw3-env.tar.gz --force

echo "Built hw3-env.tar.gz"
echo "Use with spark-submit:"
echo "  --archives hw3-env.tar.gz#env"
echo "  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./env/bin/python"
