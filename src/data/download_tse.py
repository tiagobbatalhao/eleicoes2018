import os
import requests
import zipfile
from io import BytesIO
import gzip
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

from connections.gcp_storage import GCP_storage

def list_states():
    states = [
        'SP', 'MG', 'ES', 'RJ',
        'PR', 'SC', 'RS',
        'MT', 'MS', 'GO', 'DF',
        'AC', 'AM', 'AP', 'PA', 'RO', 'RR', 'TO',
        'MA', 'CE', 'PI', 'RN', 'PB', 'PE', 'AL', 'SE', 'BA',
        'ZZ', 'BR', 'VT',
    ]
    return states

def list_urls():
    """
    List URLS for downloading files from Repositorio
    """
    urls = {}
    urls['candidatos'] = [
        'http://agencia.tse.jus.br/estatistica/sead/odsele/consulta_cand/consulta_cand_2018.zip',
        'http://agencia.tse.jus.br/estatistica/sead/odsele/bem_candidato/bem_candidato_2018.zip',
        'http://agencia.tse.jus.br/estatistica/sead/odsele/consulta_coligacao/consulta_coligacao_2018.zip',
        'http://agencia.tse.jus.br/estatistica/sead/odsele/consulta_vagas/consulta_vagas_2018.zip',
        'http://agencia.tse.jus.br/estatistica/sead/odsele/motivo_cassacao/motivo_cassacao_2018.zip',
    ]
    urls['eleitorado'] = [
        'http://agencia.tse.jus.br/estatistica/sead/odsele/perfil_eleitorado/perfil_eleitorado_2018.zip',
        'http://agencia.tse.jus.br/estatistica/sead/odsele/perfil_eleitor_deficiente/perfil_eleitor_deficiencia_2018.zip',
        'http://agencia.tse.jus.br/estatistica/sead/odsele/perfil_eleitor_secao/perfil_eleitor_secao_2018_{state}.zip',
    ]
    urls['resultados'] = [
        'http://agencia.tse.jus.br/estatistica/sead/odsele/votacao_candidato_munzona/votacao_candidato_munzona_2018.zip',
        'http://agencia.tse.jus.br/estatistica/sead/odsele/votacao_partido_munzona/votacao_partido_munzona_2018.zip',
        'http://agencia.tse.jus.br/estatistica/sead/odsele/votacao_secao/votacao_secao_2018_{state}.zip',
        'http://agencia.tse.jus.br/estatistica/sead/odsele/detalhe_votacao_munzona/detalhe_votacao_munzona_2018.zip',
        'http://agencia.tse.jus.br/estatistica/sead/odsele/detalhe_votacao_secao/detalhe_votacao_secao_2018.zip',
    ]
    urls['prestacao_eleitoral'] = [
        'http://agencia.tse.jus.br/estatistica/sead/odsele/prestacao_contas/prestacao_de_contas_eleitorais_orgaos_partidarios_2018.zip',
        'http://agencia.tse.jus.br/estatistica/sead/odsele/prestacao_contas/prestacao_de_contas_eleitorais_candidatos_2018.zip',
        'http://agencia.tse.jus.br/estatistica/sead/odsele/prestacao_contas/CNPJ_campanha_2018.zip',
    ]
    urls['pesquisas'] = [
        'http://agencia.tse.jus.br/estatistica/sead/odsele/pesquisa_eleitoral/pesquisa_eleitoral_2018.zip',
        'http://agencia.tse.jus.br/estatistica/sead/odsele/pesquisa_eleitoral/nota_fiscal_2018.zip',
        'http://agencia.tse.jus.br/estatistica/sead/odsele/pesquisa_eleitoral/bairro_municipio_2018.zip',
        'http://agencia.tse.jus.br/estatistica/sead/odsele/pesquisa_eleitoral/questionario_pesquisa_2018.zip',
    ]
    return urls


def create_path(label, link):
    """
    Create an appropriate path to save files
    """
    start = 'http://agencia.tse.jus.br/estatistica/sead/odsele/'
    if not link.startswith(start):
        return None
    path = os.path.join(label, os.path.dirname(link[len(start):]))
    return path


def save_link(label, link):
    """
    Download a zip file, expand it and convert csv files to gzip
    """

    base_path = os.path.join('data', 'raw', create_path(label, link))

    logging.info(f'Requesting link {link}')
    req = requests.get(link)
    logging.info(f'Done')
    logging.info(f'Reading zip file...')
    try:
        zip = zipfile.ZipFile(BytesIO(req.content))
        logging.info(f'Done')
    except zipfile.BadZipFile:
        logging.info(f'Not a valid file')
        return None

    for fl in zip.namelist():
        if fl.endswith('.csv') or fl.endswith('.txt'):
            fname = os.path.join(base_path, fl + '.gz')
            with zip.open(fl) as f2:
                compressed = gzip.compress(f2.read())
            GCP_storage().save_object(BytesIO(compressed),fname)
            logging.info(f'Saved {fname}')
        else:
            fname = os.path.join(base_path, fl)
            with zip.open(fl) as f2:
                GCP_storage().save_object(f2 ,fname)
            logging.info(f'Saved {fname}')

def main():
    saved_file = '.saved'
    try:
        with open(saved_file, 'r') as fl:
            url_done = [x.strip('\n') for x in fl.readlines()]
    except FileNotFoundError:
        url_done = []
    for label, list_url in list_urls().items():
        for url_pattern in list_url:
            if '{state}' in url_pattern:
                urls = [url_pattern.format(state=state) for state in list_states()]
            else:
                urls = [url_pattern]
            for url in urls:
                if url not in url_done:
                    save_link(label, url)
                    with open(saved_file, 'a') as fl:
                        fl.write(url)

if __name__=='__main__':
    main()
