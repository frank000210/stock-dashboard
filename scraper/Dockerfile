FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mops_scraper.py .

CMD ["python", "mops_scraper.py", "--watch"]
