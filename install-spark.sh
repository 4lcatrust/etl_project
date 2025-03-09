#!/bin/bash
set -e

# Install Spark if it doesn't exist
if [ ! -d "/opt/spark" ]; then
  echo "Spark not found. Installing..."
  
  mkdir -p /opt/spark
  cd /tmp
  
  wget -v "https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz"
  tar -xzf "spark-3.5.0-bin-hadoop3.tgz"
  cp -r spark-3.5.0-bin-hadoop3/* /opt/spark/
  rm "spark-3.5.0-bin-hadoop3.tgz"
  chmod -R 755 /opt/spark
  
  echo "Spark installation complete"
fi

# Verify installation
echo "Spark installation verification:"
ls -la /opt/spark/
ls -la /opt/spark/bin/