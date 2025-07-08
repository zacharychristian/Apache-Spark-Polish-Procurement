# Use Spark + Python + OpenJDK base image
FROM bitnami/spark:3.5.1

# Set working directory
WORKDIR /app

# Install required system packages
USER root
RUN apt-get update && apt-get install -y \
    python3-pip \
    git \
    curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install required Python packages
RUN pip3 install --upgrade pip && \
    pip3 install \
        pandas \
        findspark \
        "pyarrow>=4.0.0" \
        git+https://github.com/nidhaloff/deep-translator.git

# Install PostgreSQL JDBC driver (compatible with PostgreSQL 17.5)
ENV POSTGRES_JDBC_VERSION=42.7.3
RUN mkdir -p /opt/spark/jars && \
    curl -o /opt/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar \
    https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar

# Set Spark classpath so it can use the JDBC driver
ENV SPARK_CLASSPATH="/opt/spark/jars/*"

# Copy your Spark application
COPY polish_api_to_db.py .

# Run Spark locally with all cores by default
CMD ["spark-submit", "--master", "local[*]", "--jars", "/opt/spark/jars/postgresql-42.7.3.jar", "polish_api_to_db.py"]
