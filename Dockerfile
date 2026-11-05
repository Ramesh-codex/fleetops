FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml ./
COPY fleetops ./fleetops
RUN pip install --no-cache-dir .
ENV FLEETOPS_SPEEDUP=10 FLEETOPS_BUSES=12
EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=3s CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"
CMD ["uvicorn", "fleetops.api:app", "--host", "0.0.0.0", "--port", "8000"]
