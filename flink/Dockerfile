FROM flink:1.20.2

# Download JARs first
RUN wget -P /opt/flink/lib/ https://jdbc.postgresql.org/download/postgresql-42.7.7.jar
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/3.3.0-1.20/flink-connector-jdbc-3.3.0-1.20.jar
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar

RUN chown -R flink:flink /opt/flink/lib

ENV APP_HOME=/opt/flink/app

WORKDIR $APP_HOME

## Install system dependencies: Python toolchain + JDK (needed to build pemja when installing PyFlink)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk \
        python3 python3-pip python3-dev \
        build-essential curl unzip && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    # Create the directory layout pemja expects (/opt/java/openjdk/include)
    JAVA_DIR=$(dirname $(dirname $(readlink -f $(which javac)))) && \
    mkdir -p /opt/java && ln -s "$JAVA_DIR" /opt/java/openjdk || true && \
    # Ensure include directory exists at the expected path for pemja
    if [ ! -d /opt/java/openjdk/include ]; then \
        mkdir -p /opt/java/openjdk/include && cp -r "$JAVA_DIR"/include/* /opt/java/openjdk/include/; \
    fi && \
    ln -s "$JAVA_DIR" /usr/lib/jvm/java-17-openjdk-amd64 || true && \
    ln -s "$JAVA_DIR" /usr/lib/jvm/java-17-openjdk-arm64 || true && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Resolve JAVA_HOME (OpenJDK 17) and export
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="$JAVA_HOME/bin:${PATH}"

# Copy requirements and install Python packages (as root)
COPY requirements.txt .
RUN pip3 install --no-cache-dir --disable-pip-version-check -r requirements.txt