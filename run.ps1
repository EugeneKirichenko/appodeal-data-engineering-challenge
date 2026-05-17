$ErrorActionPreference = "Continue"

$docker = Get-Command docker -ErrorAction SilentlyContinue
if (-not $docker) {
    Write-Host "Docker is not installed."
    Write-Host "Please install Docker Desktop: https://www.docker.com/products/docker-desktop/"
    exit 1
}

New-Item -ItemType Directory -Force -Path "output" | Out-Null

Write-Host "Building Docker image..."
& docker build --progress=plain -t appodeal-de-challenge .

if ($LASTEXITCODE -ne 0) {
    Write-Host "Docker build failed. Please make sure Docker Desktop is running."
    exit $LASTEXITCODE
}

Write-Host "Running pipeline..."
& docker run --rm -v "${PWD}/output:/app/output" appodeal-de-challenge

if ($LASTEXITCODE -ne 0) {
    Write-Host "Docker run failed."
    exit $LASTEXITCODE
}

Write-Host "Done. Results are available in the output folder."