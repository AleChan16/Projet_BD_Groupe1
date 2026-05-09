FROM python:3.11-bookworm
 
ENV SPARK_VERSION=3.5.8
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262
ENV SEAWEEDFS_CLIENT_VERSION=4.00
ENV OPENSEARCH_SPARK_VERSION=1.3.0
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
ENV SPARK_MASTER_PORT=7077
ENV SPARK_MASTER_HOST=spark-master
 
# Install JDK
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
 
# Install Python dependencies for PySpark
RUN pip install --no-cache-dir \
    pyspark==3.5.8 \
    boto3 \
    pandas \
    pyarrow \
    opensearch-py
 
# Download and extract Spark
RUN mkdir -p ${SPARK_HOME} \
    && curl -fsSL "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
       | tar xz --directory ${SPARK_HOME} --strip-components 1
 
# --- Fichiers JAR nécessaires pour se connecter à SeaweedFS (S3) ---
# hadoop-aws: implémente le protocole s3a:// en Hadoop/Spark
# aws-java-sdk-bundle: bibliothèque AWS utilisée en interne par hadoop-aws
RUN curl -fsSL -o ${SPARK_HOME}/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
       "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
    && curl -fsSL -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
       "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"
 
# --- JAR client SeaweedFS pour Hadoop ---
# Optimise spécifiquement la communication s3a:// avec SeaweedFS 
# Sans ce fichier JAR, Spark utilise hadoop-aws générique, qui peut s'avérer instable avec SeaweedFS
RUN curl -fsSL -o ${SPARK_HOME}/jars/seaweedfs-hadoop3-client-${SEAWEEDFS_CLIENT_VERSION}.jar \
       "https://repo1.maven.org/maven2/com/seaweedfs/seaweedfs-hadoop3-client/${SEAWEEDFS_CLIENT_VERSION}/seaweedfs-hadoop3-client-${SEAWEEDFS_CLIENT_VERSION}.jar"
 
# --- Connecteur OpenSearch JAR pour Spark ---
# Permet d'écrire des DataFrames directement dans des index OpenSearch
# depuis PySpark avec : df.write.format(« opensearch »).save(« index/type »)
RUN curl -fsSL -o ${SPARK_HOME}/jars/opensearch-spark-30_2.12-${OPENSEARCH_SPARK_VERSION}.jar \
       "https://repo1.maven.org/maven2/org/opensearch/client/opensearch-spark-30_2.12/${OPENSEARCH_SPARK_VERSION}/opensearch-spark-30_2.12-${OPENSEARCH_SPARK_VERSION}.jar"
 
WORKDIR ${SPARK_HOME}
COPY ./spark-defaults-dockerfile.conf "${SPARK_HOME}/conf/spark-defaults.conf"
 
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
  CMD curl -f http://localhost:8080/ || curl -f http://localhost:8081/ || curl -f http://localhost:18080/ || exit 1
 
ENTRYPOINT ["/bin/bash"]
