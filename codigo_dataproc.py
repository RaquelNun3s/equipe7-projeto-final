from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage
import re
import os

ServiceAccount = r'C:\Users\quel-\OneDrive\Área de Trabalho\CURSOS\Curso - Python\Atividades_aulas_aovivo\Atividades\Projeto_Individual\striking-shadow-349020-c0b04aecfb6a.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ServiceAccount

def create_cluster(project_id, region, cluster_name):
    """This sample walks a user through creating a Cloud Dataproc cluster
    using the Python client library.

    Args:
        project_id (string): Project to use for creating resources.
        region (string): Region where the resources should live.
        cluster_name (string): Name to use for creating a cluster.
    """

    # Create a client with the endpoint set to the desired cluster region.
    cluster_client = dataproc.ClusterControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    # Create the cluster config.
    cluster = {
        "project_id": project_id,
        "cluster_name": cluster_name,
        "config": {
            "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-2"},
            "worker_config": {"num_instances": 2, "machine_type_uri": "n1-standard-2"},
        },
    }

    # Create the cluster.
    operation = cluster_client.create_cluster(
        request={"project_id": project_id, "region": region, "cluster": cluster}
    )
    result = operation.result()

    # Output a success message.
    print(f"Cluster created successfully: {result.cluster_name}")
    
    
create_cluster("striking-shadow-349020", "us-central1", "testeeee")

cluster_name = "testeeee"
project_id = "striking-shadow-349020"
region = "us-central1"
gcs_bucket = "projeto-individual"
spark_filename = "job1.py"


# Create the job client.
job_client = dataproc.JobControllerClient(
    client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
)

# Create the job config.
job = {
    "placement": {"cluster_name": cluster_name},
    "pyspark_job": {"main_python_file_uri": "gs://{}/{}".format(gcs_bucket, spark_filename)},
}

operation = job_client.submit_job_as_operation(
    request={"project_id": project_id, "region": region, "job": job}
)
response = operation.result()

# Dataproc job output is saved to the Cloud Storage bucket
# allocated to the job. Use regex to obtain the bucket and blob info.
matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)

output = (
    storage.Client()
    .get_bucket(matches.group(1))
    .blob(f"{matches.group(2)}.000000000")
    .download_as_string()
)

print(f"Job finished successfully: {output}\r\n")

#Delete the cluster once the job has terminated.
cluster_client = dataproc.ClusterControllerClient(
        client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
    )
operation = cluster_client.delete_cluster(
    request={
        "project_id": project_id,
        "region": region,
        "cluster_name": cluster_name,
    }
)
operation.result()

print("Cluster {} successfully deleted.".format(cluster_name))