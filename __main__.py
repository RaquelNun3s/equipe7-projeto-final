from dataproc import *
import os

ServiceAccount = r'C:\Users\quel-\OneDrive\Área de Trabalho\CURSOS\Curso - Python\Projeto_final\projeto-mineracao-soulcode-85d1cc17951f.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ServiceAccount

cluster_name = "job1"
project_id = "projeto-mineracao-soulcode"
region = "us-east1" # Importante que seja a mesma região padrão do seu projeto
gcs_bucket = "soulcode-mineracao"
spark_filename = "job1.py"

cluster = Dataproc(cluster_name, region, project_id)
cluster.cria_cluster()

job1 = Job(region, project_id, gcs_bucket, spark_filename, cluster_name)
job1.cria_job()

cluster.deleta_cluster()