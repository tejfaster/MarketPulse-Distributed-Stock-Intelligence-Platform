Decision 1: NFS vs Symlink for shared file paths
- Tried: NFS between Mac and Windows WSL2
- Problem: WSL2 virtual network (172.20.x.x) blocked NFS auth
- Decision: Symlink on Windows pointing Mac path → Windows path
- Reason: Simple, permanent, no network dependency

Decision 2: dbt vs PySpark for Gold layer
- Tried: dbt for SQL transformations
- Problem: dbt can't read Delta files directly, redundant with Spark
- Decision: PySpark writes directly to PostgreSQL
- Reason: Spark already transforms data, dbt adds no value here

Decision 3: Python version on Windows Worker
- Tried: .bashrc, spark-env.sh, spark.pyspark.python config
- Problem: Worker process doesn't load shell environments
- Decision: update-alternatives to make python3.11 system default
- Reason: Only system default is picked up by Java-launched processes