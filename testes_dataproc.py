from dataproc import *
import os

ServiceAccount = r'C:\Users\quel-\OneDrive\Área de Trabalho\CURSOS\Curso - Python\Atividades_aulas_aovivo\Atividades\Projeto_Individual\striking-shadow-349020-c0b04aecfb6a.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ServiceAccount

cluster_name = "testeeee"
project_id = "striking-shadow-349020"
region = "us-central1"
gcs_bucket = "projeto-individual"
spark_filename = "job1.py"

cluster = Dataproc(cluster_name, region, project_id)
#cluster.cria_cluster()

job1 = Job(region, project_id, gcs_bucket, spark_filename, cluster_name)
#job1.cria_job()

cluster.deleta_cluster()