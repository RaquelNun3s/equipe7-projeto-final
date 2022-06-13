'''

As linhas de código abaixo devem ser executadas pelo terminal de comando.
Após instalação prossiga para importação das bibliotecas.

import os

pip install apache-beam[interactive]
pip install apache-beam[gcp]

'''

'''Importação das bibliotecas necessárias:'''
import pandas as pd
import os
from google.cloud import storage
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

'''Definindo o template da pipeline:'''
pipeline_options_template = {
    'project':'projeto-mineracao-soulcode',
    'runner':'DataflowRunner',
    'region':'southamerica_east1',
    'temp_location':'gs://soulcode-mineracao/staging',
    'template_location':'gs://soulcode-mineracao/templates/template_diamantes'
}

'''Instanciando o template, carregando um dicionário com chaves e valores:'''
pipeline_options = PipelineOptions.from_dictionary(pipeline_options_template)

'''Construção da pipeline:'''
p1 = beam.Pipeline(options=pipeline_options)

diamantes = (
    p1
    |'Leitura do dataset'>> beam.io.ReadFromText('gs://soulcode-mineracao/tratados/arrecadacao.csv', skip_header_lines=1)
    |'Separar por virgula'>> beam.Map(lambda record: record.split(','))
    |'Filtrar ano' >> beam.Filter(lambda record: int(record[0])>2011)
    |'Agregar as colunas' >> beam.Map(lambda record: (int(record[0]), record[5], record[6], record[8], record[9]))
    |'Filtrar por material' >> beam.Filter(lambda record: record[1] == 'DIAMANTE')
    #|'Exibir o resultado' >> beam.Map(print)
    |'Load para arquivo' >> beam.io.WriteToText('gs://soulcode-mineracao/pipelines_outputs/diamantes_ultimos_dez_anos', file_name_suffix='.csv', num_shards=1)
)

'''Executando a pipeline criada:'''
p1.run()