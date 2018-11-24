# eleicoes2018
Análise de dados públicos sobre os resultados das eleições 2018.

## Como instalar

Para usar esse repositório, é recomendado Python3 e ter instalado a interface de linha de comando do Google Cloud Project.
```
mkvirtualenv -ppython3 eleicoes2018
pip install -r requirements.txt
```

Além disso, você deve setar as variáveis de ambiente
```
export GOOGLE_APPLICATION_CREDENTIALS=<arquivo com credenciais do GCP>
export GCP_PROJECT=<projeto no GCP>
export GCP_STORAGE_BUCKET=<bucket no GCS>
export PYTHONPATH=$PYTHONPATH:.
```

## Baixando os dados

O script `src/data/download_tse` baixa uma lista de arquivos do repositório oficial do TSE e salva em um bucket no Google Cloud Storage.
