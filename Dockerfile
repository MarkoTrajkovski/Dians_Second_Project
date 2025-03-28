FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y default-jdk procps curl && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /drivers && \
    curl -o /drivers/postgresql-42.7.2.jar https://jdbc.postgresql.org/download/postgresql-42.7.2.jar

ENV PYTHONUNBUFFERED=1

# ✅ Add these two lines:
EXPOSE 8050
CMD ["python", "dash_app/visualization.py"]

