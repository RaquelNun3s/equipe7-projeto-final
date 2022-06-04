from pyspark.sql import SparkSession
from pyspark import SparkConf
import pyspark.sql.functions as F
from google.cloud import storage
import os

path = 'gs://projeto-individual/marketing_campaign.csv'
spark = ( SparkSession.builder
                        .master("local")
                        .appName("aula-inicial")
                        .config("spark.ui.port", "4050")
                        .config("spark.jars", 'https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-latest.jar')
                        .getOrCreate() 
        )
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

dfs_arrecadacao = (spark.read.format("csv")
                  .option("header", "true")
                  .option("inferschema", "true")
                  .option("delimiter", ";")
                  .option("encoding", "latin1")
                  .load(path)
)

dfs_arrecadacao.coalesce(1).write.parquet("gs://projeto-individual/marketing2.parquet")