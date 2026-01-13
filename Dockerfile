FROM python:3.13-slim

WORKDIR /app

# Install uv for faster dependency resolution (optional but good practice as per user setup)
# or just install from pyproject.toml
# Since uv.lock exists, maybe use it? But to keep it simple and portable without installing uv inside if not needed:
# Convert pyproject.toml dependencies to requirements.txt-style install
# The user's pyproject.toml has simple dependencies.

RUN pip install --no-cache-dir \
    asyncclick>=8.3.0.7 \
    better-exceptions>=0.3.3 \
    influxdb3-python>=0.16.0 \
    pendulum>=3.1.0 \
    python-graphql-client>=0.4.3

COPY . .

# Default command
CMD ["python", "main.py", "serve"]
