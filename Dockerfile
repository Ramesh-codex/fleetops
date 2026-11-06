FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml ./
COPY fleetops ./fleetops
RUN pip install --no-cache-dir .
ENV FLEETOPS_SPEEDUP=10 FLEETOPS_BUSES=12 \
    FLEETOPS_HEALTH_URL=http://localhost:8000/health
EXPOSE 8000
# One image runs the API, the consumer and the producer. Each service sets
# FLEETOPS_HEALTH_URL to an endpoint it actually serves (the consumer has no
# /health on 8000, only metrics on 9108), or disables the check in compose.
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s \
  CMD python -c "import os, urllib.request; urllib.request.urlopen(os.environ['FLEETOPS_HEALTH_URL'])"
CMD ["uvicorn", "fleetops.api:app", "--host", "0.0.0.0", "--port", "8000"]
