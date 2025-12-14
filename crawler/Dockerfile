FROM python:latest
RUN pip install "psycopg[binary,pool]"
COPY . /app/
WORKDIR /app
CMD ["python", "/app/cloud_sql_connect_test.py"]

