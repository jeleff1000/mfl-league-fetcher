FROM python:3.11-slim

WORKDIR /app
COPY duckdb-server/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY duckdb-server/*.py .
# The merge endpoint calls the same canonical aggregations as imports. The
# Dockerfile-specific allowlist admits Python source only, never league/NFL data.
COPY fantasy_football_data_scripts/multi_league ./multi_league
RUN python -c "from multi_league.transformations.aggregation import aggregate_matchup_context, aggregate_fantasy_context, aggregate_draft_context, aggregate_transaction_context"

# Data directory (mounted as Fly volume)
RUN mkdir -p /data

EXPOSE 8080
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1", "--limit-concurrency", "64"]
