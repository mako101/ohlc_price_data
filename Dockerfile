FROM python:3.9-alpine


RUN apk update && apk add bash openjdk11
RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy the Python script to generate data
COPY csv_processor/ /app/csv_processor/

# Read MYSQL password from the environment
ENV MYSQL_PWD=${MYSQL_PWD}
ENV PYTHONPATH=${PYTHONPATH}:/app
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk
# Expose Application port
EXPOSE 5000

WORKDIR /app/csv_processor/

# Start the application
CMD ["python3", "run.py"]
