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
            cluster = {
                "project_id": self.project_id,
                "cluster_name": self.nome_cluster,
                "config": {
                    "master_config": {
                        "num_instances": 1,
                        "machine_type_uri": "n1-standard-2",
                        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 100},
                    },
                    "worker_config": {
                        "num_instances": 2,
                        "machine_type_uri": "n1-standard-2",
                        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 100},
                    },},}
            
                
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
