from datetime import datetime, date
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
from connections.gcp_storage import GCP_storage
import pandas

def list_files():
    gcs = GCP_storage()
    files = {
        'zona': gcs.glob('data/raw/eleitorado/perfil_eleitorado/*.csv.gz'),
        'secao': gcs.glob('data/raw/eleitorado/perfil_eleitor_secao/*.csv.gz'),
        'deficiente': gcs.glob('data/raw/eleitorado/perfil_eleitor_deficiente/*.csv.gz'),
     }
    return files

def read_files(file_list):
    gcs = GCP_storage()
    for fl in file_list:
        if 'BRASIL' not in fl:
            logging.info(f'Reading file {fl}...')
            gen = gcs.read_gzip_file(fl)
            logging.info(f'Done.')
            next(gen)
            for line in gen:
                yield [x.strip('"') for x in line.split(';')]

def parse_general(file_list, relevant):
    columns = [x[2] for x in relevant]
    values = []
    for line in read_files(file_list):
        this = []
        for index, name_orig, name_new, typ in relevant:
            value = line[index]
            if typ == 'integer':
                value = int(value)
            elif typ == 'float':
                value = float(value.replace(',','.'))
            elif typ == 'date':
                value = datetime.strptime(value, '%d/%m/%Y').date()
            elif typ == 'datetime':
                value = datetime.strptime(value, '%d/%m/%Y')
            elif typ == 'boolean':
                value = value == 'S'
            elif typ == 'string':
                value = value.upper().strip()
            if typ == 'string' and (value.startswith('#NULO') or value.startswith('#NE')):
                value = None
            this.append(value)
        values.append(this)
    return columns, values

def parse_zona():
    columns_all = [
        (0, 'DT_GERACAO', '', ''),
        (1, 'HH_GERACAO', '', ''),
        (2, 'ANO_ELEICAO', '', ''),
        (3, 'SG_UF', '', ''),
        (4, 'CD_MUNICIPIO', '', ''),
        (5, 'NM_MUNICIPIO', '', ''),
        (6, 'CD_MUN_SIT_BIOMETRIA', '', ''),
        (7, 'DS_MUN_SIT_BIOMETRIA', '', ''),
        (8, 'NR_ZONA', '', ''),
        (9, 'CD_GENERO', '', ''),
        (10, 'DS_GENERO', '', ''),
        (11, 'CD_ESTADO_CIVIL', '', ''),
        (12, 'DS_ESTADO_CIVIL', '', ''),
        (13, 'CD_FAIXA_ETARIA', '', ''),
        (14, 'DS_FAIXA_ETARIA', '', ''),
        (15, 'CD_GRAU_ESCOLARIDADE', '', ''),
        (16, 'DS_GRAU_ESCOLARIDADE', '', ''),
        (17, 'QT_ELEITORES_PERFIL', '', ''),
        (18, 'QT_ELEITORES_BIOMETRIA', '', ''),
        (19, 'QT_ELEITORES_DEFICIENCIA', '', ''),
        (20, 'QT_ELEITORES_INC_NM_SOCIAL', '', ''),
    ]

    relevant = [
        (2, 'ANO_ELEICAO', 'Eleição_Ano', 'integer'),
        (3, 'SG_UF', 'Região_UF', 'string'),
        (4, 'CD_MUNICIPIO', 'Região_Município', 'integer'),
        (5, 'NM_MUNICIPIO', 'Região_Município_Nome', 'string'),
        (7, 'DS_MUN_SIT_BIOMETRIA', 'Região_Biometria', 'string'),
        (8, 'NR_ZONA', 'Região_Zona', 'integer'),
        (10, 'DS_GENERO', 'Demografia_Gênero', 'string'),
        (12, 'DS_ESTADO_CIVIL', 'Demografia_EstadoCivil', 'string'),
        (14, 'DS_FAIXA_ETARIA', 'Demografia_Idade', 'string'),
        (16, 'DS_GRAU_ESCOLARIDADE', 'Demografia_Instrução', 'string'),
        (17, 'QT_ELEITORES_PERFIL', 'Quantidade', 'integer'),
        (18, 'QT_ELEITORES_BIOMETRIA', 'Quantidade_Biometria', 'integer'),
        (19, 'QT_ELEITORES_DEFICIENCIA', 'Quantidade_Deficiência', 'integer'),
        (20, 'QT_ELEITORES_INC_NM_SOCIAL', 'Quantidade_NomeSocial', 'integer'),
    ]

    return parse_general(list_files()['zona'], relevant)

def parse_secao():
    columns_all = [
        (0, 'DT_GERACAO', '', ''),
        (1, 'HH_GERACAO', '', ''),
        (2, 'ANO_ELEICAO', '', ''),
        (3, 'SG_UF', '', ''),
        (4, 'CD_MUNICIPIO', '', ''),
        (5, 'NM_MUNICIPIO', '', ''),
        (6, 'CD_MUN_SIT_BIOMETRIA', '', ''),
        (7, 'DS_MUN_SIT_BIOMETRIA', '', ''),
        (8, 'NR_ZONA', '', ''),
        (9, 'NR_SECAO', '', ''),
        (10, 'CD_GENERO', '', ''),
        (11, 'DS_GENERO', '', ''),
        (12, 'CD_ESTADO_CIVIL', '', ''),
        (13, 'DS_ESTADO_CIVIL', '', ''),
        (14, 'CD_FAIXA_ETARIA', '', ''),
        (15, 'DS_FAIXA_ETARIA', '', ''),
        (16, 'CD_GRAU_ESCOLARIDADE', '', ''),
        (17, 'DS_GRAU_ESCOLARIDADE', '', ''),
        (18, 'QT_ELEITORES_PERFIL', '', ''),
        (19, 'QT_ELEITORES_BIOMETRIA', '', ''),
        (20, 'QT_ELEITORES_DEFICIENCIA', '', ''),
        (21, 'QT_ELEITORES_INC_NM_SOCIAL', '', ''),
    ]

    relevant = [
        (2, 'ANO_ELEICAO', 'Eleição_Ano', 'integer'),
        (4, 'CD_MUNICIPIO', 'Região_Município_Código', 'integer'),
        (8, 'NR_ZONA', 'Região_Zona', 'integer'),
        (9, 'NR_SECAO', 'Região_Seção', 'integer'),
        (11, 'DS_GENERO', 'Demografia_Gênero', 'string'),
        (13, 'DS_ESTADO_CIVIL', 'Demografia_EstadoCivil', 'string'),
        (15, 'DS_FAIXA_ETARIA', 'Demografia_Idade', 'string'),
        (17, 'DS_GRAU_ESCOLARIDADE', 'Demografia_Instrução', 'string'),
        (18, 'QT_ELEITORES_PERFIL', 'Quantidade', 'integer'),
        (19, 'QT_ELEITORES_BIOMETRIA', 'Quantidade_Biometria', 'integer'),
        (20, 'QT_ELEITORES_DEFICIENCIA', 'Quantidade_Deficiência', 'integer'),
        (21, 'QT_ELEITORES_INC_NM_SOCIAL', 'Quantidade_NomeSocial', 'integer'),
    ]

    return parse_general(list_files()['secao'], relevant)


def parse_deficiente():
    columns_all = [
        (0, 'DT_GERACAO', '', ''),
        (1, 'HH_GERACAO', '', ''),
        (2, 'ANO_ELEICAO', '', ''),
        (3, 'SQ_ELEITOR', '', ''),
        (4, 'SG_UF', '', ''),
        (5, 'CD_MUNICIPIO', '', ''),
        (6, 'NM_MUNICIPIO', '', ''),
        (7, 'CD_MUN_SIT_BIOMETRIA', '', ''),
        (8, 'DS_MUN_SIT_BIOMETRIA', '', ''),
        (9, 'NR_ZONA', '', ''),
        (10, 'NR_SECAO', '', ''),
        (11, 'CD_GENERO', '', ''),
        (12, 'DS_GENERO', '', ''),
        (13, 'CD_ESTADO_CIVIL', '', ''),
        (14, 'DS_ESTADO_CIVIL', '', ''),
        (15, 'CD_FAIXA_ETARIA', '', ''),
        (16, 'DS_FAIXA_ETARIA', '', ''),
        (17, 'CD_GRAU_ESCOLARIDADE', '', ''),
        (18, 'DS_GRAU_ESCOLARIDADE', '', ''),
        (19, 'ST_ELEITOR_BIOMETRIA', '', ''),
        (20, 'CD_TIPO_DEFICIENCIA', '', ''),
        (21, 'DS_TIPO_DEFICIENCIA', '', ''),
    ]

    relevant = [
        (2, 'ANO_ELEICAO', 'Eleição_Ano', 'integer'),
        (3, 'SQ_ELEITOR', 'Eleitor_id', 'string'),
        (5, 'CD_MUNICIPIO', 'Região_Município', 'integer'),
        (9, 'NR_ZONA', 'Região_Zona', 'integer'),
        (10, 'NR_SECAO', 'Região_Seção', 'integer'),
        (12, 'DS_GENERO', 'Demografia_Gênero', 'string'),
        (14, 'DS_ESTADO_CIVIL', 'Demografia_EstadoCivil', 'string'),
        (16, 'DS_FAIXA_ETARIA', 'Demografia_Idade', 'string'),
        (18, 'DS_GRAU_ESCOLARIDADE', 'Demografia_Instrução', 'string'),
        (19, 'ST_ELEITOR_BIOMETRIA', 'Eleitor_Biometria', 'boolean'),
        (21, 'DS_TIPO_DEFICIENCIA', 'Eleitor_Deficiência', 'string'),
    ]

    return parse_general(list_files()['deficiente'], relevant)


def main():
    aux = parse_zona()
    df_zona = pandas.DataFrame(aux[1], columns=aux[0])

    df_municipio = df_zona.groupby('Região_Município', as_index=False).agg({
        'Região_UF': 'first',
        'Região_Município_Nome': 'first',
        'Região_Biometria': 'first',
    }).rename(columns={
        'Região_Município': 'id',
        'Região_UF': 'UF',
        'Região_Município_Nome': 'Nome',
        'Região_Biometria': 'Biometria',
    })

    df_zona.drop(
        ['Região_UF', 'Região_Município_Nome', 'Região_Biometria'],
        axis=1, inplace=True
    )

    aux = parse_secao()
    df_secao = pandas.DataFrame(aux[1], columns=aux[0])

    aux = parse_deficiente()
    df_deficiente = pandas.DataFrame(aux[1], columns=aux[0])

    return {
        'perfil_zona': df_zona,
        'municipio': df_municipio,
        'perfil_secao': df_secao,
        'perfil_deficiente': df_deficiente,
    }
