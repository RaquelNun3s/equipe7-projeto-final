from dataproc import *
import os
from google.cloud import storage

ServiceAccount = r'' # Path do arquivo chave da Service Account
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ServiceAccount

# Enviando os arquivos dos jobs e do script de inicialização para o bucket:
client = storage.Client()
bucket = client.get_bucket('soulcode-mineracao')
blob1 = bucket.blob('job1.py')
blob2 = bucket.blob('job2.py')
blob3 = bucket.blob('job3.py')
blob4 = bucket.blob('script_inicializacao.sh')

blob1.upload_from_filename(r'C:\Users\quel-\OneDrive\Área de Trabalho\novaBranch\Jobs\job1.py')
blob2.upload_from_filename(r'C:\Users\quel-\OneDrive\Área de Trabalho\novaBranch\Jobs\job2.py')
blob3.upload_from_filename(r'C:\Users\quel-\OneDrive\Área de Trabalho\novaBranch\Jobs\job3.py')
blob4.upload_from_filename(r'C:\Users\quel-\OneDrive\Área de Trabalho\novaBranch\script_inicializacao.sh')

# Criando o cluster:
cluster_name = "cluster-teste"
project_id = "projeto-mineracao-soulcode"
region = "us-east1"
gcs_bucket = "soulcode-mineracao"
zone = "us-east1-c"

cluster = Dataproc(cluster_name, region, project_id, zone)
cluster.cria_cluster()

# Enviando os jobs:
job1 = Job(region, project_id, gcs_bucket, 'job1.py', cluster_name)
job2 = Job(region, project_id, gcs_bucket, 'job2.py', cluster_name)
job3 = Job(region, project_id, gcs_bucket, 'job3.py', cluster_name)

try:
    job1.cria_job()
    job2.cria_job()
    job3.cria_job()
except Exception as e:
    print(str(e))

# Deletando o cluster para evitar cobranças
cluster.deleta_cluster()
