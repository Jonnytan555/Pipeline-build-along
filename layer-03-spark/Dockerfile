# ============================================================
# LAYER 3 — Custom Spark image
#
# WHY a custom image?
#   The vanilla bitnami/spark image cannot talk to MinIO (S3A)
#   or PostgreSQL out of the box.  We download the three JARs
#   that unlock those connections at build time so they are
#   always available — no downloading at job-submission time.
#
# JARs added:
#   hadoop-aws          → S3A file system for MinIO / AWS S3
#   aws-java-sdk-bundle → AWS SDK required by hadoop-aws
#   mssql-jdbc          → JDBC driver so Spark can write to SQL Server
#
# Version notes:
#   Spark 3.5 ships with Hadoop 3.3 libraries, so hadoop-aws
#   must be 3.3.x to avoid classpath conflicts.
# ============================================================

FROM bitnami/spark:3.5

USER root

RUN curl -fsSL -o /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && curl -fsSL -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    && curl -fsSL -o /opt/bitnami/spark/jars/mssql-jdbc-12.4.2.jre11.jar \
        https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.4.2.jre11/mssql-jdbc-12.4.2.jre11.jar

USER 1001
