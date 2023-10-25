#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
from io import BytesIO
from zipfile import ZipFile
from google.cloud import storage

'''
Script para obtenção, unzip e gravação no Blob Storage da GCP
Rodando no Dataproc até o momento
- Refatorar em POO para melhorar replicabilidade
- Mudar ordem do gate de verificação se o arquivo já existe
- Aplicar multithreading

'''

#GCP bucket dados de CONSULTA apenas
BUCKET_NAME = "ans-amb-cons"

# Listas de estados, anos e meses
states = ['AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SE', 'TO','SP']
years = [str(year) for year in range(2018, 2023)] # como não é inclusivo, preciso colocar 2023
months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

# Laço for para cada combinação de estado, ano e mês
for state in states:
    for year in years:
        for month in months:
            DESTINATION_BLOB_NAME = f'cons/{state}_{year}{month}.csv'
            URL = f"https://dadosabertos.ans.gov.br/FTP/PDA/TISS/AMBULATORIAL/{year}/{state}/{state}_{year}{month}_AMB_CONS.zip"

            try:
                response = requests.get(URL)
                response.raise_for_status()  # caso o Response não seja 200, aponta erro

                zip_content = BytesIO(response.content)
                
                with ZipFile(zip_content) as zf:  # Fazendo o unzip in-memory -- para evitar gastar com storage do .zip
                    csv_file_name = zf.namelist()[0]
                    with zf.open(csv_file_name) as extracted_file:
                        csv_content = extracted_file.read()

                        # Fazer o upload para o GCS
                        
                        storage_client = storage.Client()
                        bucket = storage_client.bucket(BUCKET_NAME)
                        blob = bucket.blob(DESTINATION_BLOB_NAME)
                        
                        if not blob.exists():
                            blob.upload_from_string(csv_content, content_type="text/csv")
                            
                            print(f"Arquivo {csv_file_name} foi enviado para {DESTINATION_BLOB_NAME} no bucket {BUCKET_NAME}.")
                            
                        else:
                            print(f"Arquivo {DESTINATION_BLOB_NAME} já existe no bucket {BUCKET_NAME}.")

            except requests.HTTPError as e:
                print(f"Não foi possível obter o arquivo para {state}, {year}, {month}. Erro: {e}")


# In[1]:


'''
Script para obtenção, unzip e gravação no Blob Storage da GCP
Rodando no Dataproc até o momento
TO DO: 
- Refatorar em POO para melhorar replicabilidade
- Mudar ordem do gate de verificação se o arquivo já existe
- Aplicar multithreading

'''

#GCP bucket dados de DET apenas
BUCKET_NAME = "ans-amb-det"

# Listas de estados, anos e meses
states = ['SP']
years = [str(year) for year in range(2018, 2023)] # como não é inclusivo, preciso colocar 2023
months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

# Laço for para cada combinação de estado, ano e mês
for state in states:
    for year in years:
        for month in months:
            DESTINATION_BLOB_NAME = f'det/{state}_{year}{month}.csv'
            URL = f"https://dadosabertos.ans.gov.br/FTP/PDA/TISS/AMBULATORIAL/{year}/{state}/{state}_{year}{month}_AMB_DET.zip"

            try:
                response = requests.get(URL)
                response.raise_for_status()  # caso o Response não seja 200, aponta erro

                zip_content = BytesIO(response.content)
                
                with ZipFile(zip_content) as zf:  # Fazendo o unzip in-memory -- para evitar gastar com storage do .zip
                    csv_file_name = zf.namelist()[0]
                    with zf.open(csv_file_name) as extracted_file:
                        csv_content = extracted_file.read()

                        # Fazer o upload para o GCS
                        
                        storage_client = storage.Client()
                        bucket = storage_client.bucket(BUCKET_NAME)
                        blob = bucket.blob(DESTINATION_BLOB_NAME)
                        
                        if not blob.exists():
                            blob.upload_from_string(csv_content, content_type="text/csv")
                            
                            print(f"Arquivo {csv_file_name} foi enviado para {DESTINATION_BLOB_NAME} no bucket {BUCKET_NAME}.")
                            
                        else:
                            print(f"Arquivo {DESTINATION_BLOB_NAME} já existe no bucket {BUCKET_NAME}.")
                time.sleep(5)
            except requests.HTTPError as e:
                print(f"Não foi possível obter o arquivo para {state}, {year}, {month}. Erro: {e}")


# In[ ]:




