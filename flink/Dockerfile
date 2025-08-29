FROM flink:1.20.2

# Download JARs first
RUN wget -P /opt/flink/lib/ https://jdbc.postgresql.org/download/postgresql-42.7.7.jar
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/3.3.0-1.20/flink-connector-jdbc-3.3.0-1.20.jar
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar

# Flink SQL Connector for PostgreSQL CDC
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-postgres-cdc/3.4.0/flink-sql-connector-postgres-cdc-3.4.0.jar

# Make sure all JARs are owned by flink user
RUN chown -R flink:flink /opt/flink/lib

ENV APP_HOME=/opt/flink/app

WORKDIR $APP_HOME

# Install Python and pip as root user
RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-dev build-essential curl unzip gcc librdkafka-dev && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python packages (as root)
COPY requirements.txt .
RUN pip3 install --no-cache-dir --disable-pip-version-check -r requirements.txt