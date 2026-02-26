# STAGE 1: Build the binary (The Kitchen)
# Use a slim version of Python 2026 stable
FROM python:3.12-slim

# Patch OS vulnerabilities immediately
RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY Python_Streamer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the streamer code
COPY Python_Streamer/ .

# Create the mount point for your Database folder
RUN mkdir -p /app/Database

# Run the streamer
CMD ["python", "schwabStream.py"]