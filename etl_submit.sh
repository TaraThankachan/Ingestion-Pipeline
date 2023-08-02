spark-submit --master yarn --deploy-mode cluster \
--py-files ingestion_pipeline.zip \
--files conf/etl_config,conf/env_config,log4j.properties \
etl_main.py local



