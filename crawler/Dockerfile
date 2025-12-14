FROM python:latest
RUN pip install psycopg2-binary
#RUN pip install cloud-sql-python-connector[pg8000]
COPY . /app/
WORKDIR /app
CMD ["python", "/app/cloud_sql_connect_test.py"]

