FROM python:3.10-slim

WORKDIR /app

# ✅ Install Java + required tools
RUN apt-get update && \
    apt-get install -y default-jdk procps curl && \
    apt-get clean

# ✅ Set correct JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# ✅ Add Java to PATH
ENV PATH="$JAVA_HOME/bin:$PATH"

# 🐍 Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 📁 Copy project files
COPY . .

# 🔗 Download PostgreSQL JDBC driver
RUN mkdir -p /drivers && \
    curl -o /drivers/postgresql-42.7.2.jar https://jdbc.postgresql.org/download/postgresql-42.7.2.jar

# ⏱ Ensure real-time logging
ENV PYTHONUNBUFFERED=1