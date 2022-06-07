from google.cloud import storage

class Arquivo:
    def __init__(self, nome, pasta, bucket_name, dfs, tipo):
        self.nome = nome
        self.pasta = pasta
        self.bucket_name = bucket_name
        self.dfs = dfs
        self.tipo = tipo
  
    def envia_arquivo(self):
        '''
        Essa função tem como objetivo enviar e organizar arquivos no bucket do csv
        nome = nome do arquivo que vai ser enviado ex: 'dados.csv'
        pasta = nome da pasta do arquivo que será enviado, no formato 'nome_pasta/'
        bucket = nome do bucket em que o arquivo será enviado ex:'nome-bucket'
        dfs = dataframe do spark que será convertida e enviada
        tipo = tipo do arquivo que será enviado ex: csv
        '''
        # Conectando com o cloud storage
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket_name)

        # Enviando o arquivo para o bucket na pasta desejada:
        self.dfs.coalesce(1).write.option("header", True).option("encoding", "latin1").save(f'gs://{self.bucket_name}/deletar/{self.nome}', format=self.tipo)

        # Renomeando do arquivo para o nome desejado - Pegando o blob antigo:
        blobs = storage_client.list_blobs(self.bucket_name)
        for blob in blobs:
            blob_name = str(blob)
            list_blob = blob_name.split(',')
            blob_path = list_blob[1]
            if f"deletar/{self.nome}/part" in blob_path:
                blob_antigo = blob
                blob_antigo_nome = blob_path

        # Movendo o arquivo para a pasta desejada:
        blob_copy = bucket.copy_blob(blob_antigo, bucket, f'{self.pasta}/{self.nome}')

        blob_antigo_nome = blob_antigo_nome.split(" ")
        blob_antigo_nome = blob_antigo_nome[1]

        # Deletando a pasta com os arquivos anteriores:
        bucket.delete_blob(blob_antigo_nome)