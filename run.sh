#!/usr/bin/env bash
set -euo pipefail

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is not installed."
  echo "Please install Docker Desktop: https://www.docker.com/products/docker-desktop/"
  exit 1
fi

mkdir -p output

echo "Building Docker image..."
docker build --progress=plain -t appodeal-de-challenge .

echo "Running pipeline..."
docker run --rm -v "$(pwd)/output:/app/output" appodeal-de-challenge
