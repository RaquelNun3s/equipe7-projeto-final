from google.cloud import dataproc_v1

class Dataproc:
    def __init__(self, nome_cluster, regiao, project_id):
        self.nome_cluster = nome_cluster
        self.regiao = regiao
        self.project_id = project_id
    
    def cria_cluster(self):
        try:
            # Criando o cliente do cluster
            cliente_cluster = dataproc_v1.ClusterControllerClient(
                client_options={"api_endpoint": f"{self.regiao}-dataproc.googleapis.com:443"}
            )
            # Definindo as configurações do cluster:
            cluster = cluster = {
                "cluster_name": self.nome_cluster,
                "config": {
                "config_bucket": "soulcode-mineracao",
                "gce_cluster_config": {
                    "metadata": {
                    "PIP_PACKAGES": "pyspark==3.0.1",
                    "google-cloud-storage": "=1.38.0"
                    },
                    "zone_uri": "https://www.googleapis.com/compute/v1/projects/projeto-mineracao-soulcode/zones/us-east1-d"
                },
                "master_config": {
                    "disk_config": {
                    "boot_disk_size_gb": 200,
                    "boot_disk_type": "pd-standard"
                    },
                    "image_uri": "https://www.googleapis.com/compute/v1/projects/cloud-dataproc/global/images/dataproc-2-0-deb10-20220531-170200-rc01",
                    "machine_type_uri": "https://www.googleapis.com/compute/v1/projects/projeto-mineracao-soulcode/zones/us-east1-d/machineTypes/n1-standard-4",
                    "num_instances": 1,
                },
                "software_config": {
                    "image_version": "2.0.42-debian10",
                    "properties": {
                    "spark:spark.jars.packages": "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
                    "spark:spark.scheduler.mode": "FAIR",
                    }
                },
                "worker_config": {
                    "disk_config": {
                    "boot_disk_size_gb": 200,
                    "boot_disk_type": "pd-standard"
                    },
                    "image_uri": "https://www.googleapis.com/compute/v1/projects/cloud-dataproc/global/images/dataproc-2-0-deb10-20220531-170200-rc01",
                    "machine_type_uri": "https://www.googleapis.com/compute/v1/projects/projeto-mineracao-soulcode/zones/us-east1-d/machineTypes/n1-standard-2",
                    "num_instances": 2,
                }
                },
                "project_id": self.project_id,
                }
                
            # Criando o cluster:
            operacao = cliente_cluster.create_cluster(
                request={"project_id": self.project_id, "region": self.regiao, "cluster":cluster}
            )
            result = operacao.result()
            
            # Imprimindo a mensagem sobre a criação do cluster
            print(f"Cluster: {result.cluster_name} criado com sucesso")
        except Exception as e:
            print(str(e))
        
    def deleta_cluster(self):
        cliente_cluster = dataproc_v1.ClusterControllerClient(
            client_options={"api_endpoint": f"{self.regiao}-dataproc.googleapis.com:443"}
        )
        operacao = cliente_cluster.delete_cluster(
            request={"project_id": self.project_id, "region": self.regiao, "cluster_name": self.nome_cluster,}
        )
        print(operacao.result())
        print(f"Cluster {self.nome_cluster} successfully deleted.")

class Job(Dataproc):
    def __init__(self, regiao, project_id, bucket, arquivo_spark, nome_cluster):
        self.regiao = regiao
        self.project_id = project_id
        self.bucket = bucket
        self.arquivo_spark = arquivo_spark
        self.nome_cluster = nome_cluster
    
    def cria_job(self):
        # Criando o cliente do job:
        cliente_job = dataproc_v1.JobControllerClient(
            client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(self.regiao)}
        )
        # Configurando o job:
        job = {
            "placement": {"cluster_name": self.nome_cluster},
            "pyspark_job": {"main_python_file_uri": f"gs://{self.bucket}/{self.arquivo_spark}"},
        }
        operacao = cliente_job.submit_job_as_operation(
            request={"project_id": self.project_id, "region": self.regiao, "job": job}
        )
        response = operacao.result() 
        # A partir da próxima linha ta mudado (completar com o resto da documentação)
        print(f"Job finished successfully: {response}\r\n")

dataproc_v1.ClusterConfig
dataproc_v1.PySparkJob
dataproc_v1.ClusterOperationMetadata