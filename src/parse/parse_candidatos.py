from datetime import datetime, date
import logging
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
from connections.gcp_storage import GCP_storage
import pandas

def list_files():
    gcs = GCP_storage()
    files = {
        'candidatos': gcs.glob('data/raw/candidatos/consulta_cand/*.csv.gz'),
        'coligacao': gcs.glob('data/raw/candidatos/consulta_coligacao/*.csv.gz'),
        'cassacao': gcs.glob('data/raw/candidatos/motivo_cassacao/*.csv.gz'),
        'bens': gcs.glob('data/raw/candidatos/bem_candidato/*.csv.gz'),
        'vagas': gcs.glob('data/raw/candidatos/consulta_vagas/*.csv.gz'),
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
                value = value.upper()
            if typ == 'string' and (value.startswith('#NULO') or value.startswith('#NE')):
                value = None
            this.append(value)
        values.append(this)
    return columns, values


def parse_candidatos():
    columns_all = [
        (0, 'DT_GERACAO', ''),
        (1, 'HH_GERACAO', ''),
        (2, 'ANO_ELEICAO', ''),
        (3, 'CD_TIPO_ELEICAO', ''),
        (4, 'NM_TIPO_ELEICAO', ''),
        (5, 'NR_TURNO', ''),
        (6, 'CD_ELEICAO', ''),
        (7, 'DS_ELEICAO', ''),
        (8, 'DT_ELEICAO', ''),
        (9, 'TP_ABRANGENCIA', ''),
        (10, 'SG_UF', ''),
        (11, 'SG_UE', ''),
        (12, 'NM_UE', ''),
        (13, 'CD_CARGO', ''),
        (14, 'DS_CARGO', ''),
        (15, 'SQ_CANDIDATO', ''),
        (16, 'NR_CANDIDATO', ''),
        (17, 'NM_CANDIDATO', ''),
        (18, 'NM_URNA_CANDIDATO', ''),
        (19, 'NM_SOCIAL_CANDIDATO', ''),
        (20, 'NR_CPF_CANDIDATO', ''),
        (21, 'NM_EMAIL', ''),
        (22, 'CD_SITUACAO_CANDIDATURA', ''),
        (23, 'DS_SITUACAO_CANDIDATURA', ''),
        (24, 'CD_DETALHE_SITUACAO_CAND', ''),
        (25, 'DS_DETALHE_SITUACAO_CAND', ''),
        (26, 'TP_AGREMIACAO', ''),
        (27, 'NR_PARTIDO', ''),
        (28, 'SG_PARTIDO', ''),
        (29, 'NM_PARTIDO', ''),
        (30, 'SQ_COLIGACAO', ''),
        (31, 'NM_COLIGACAO', ''),
        (32, 'DS_COMPOSICAO_COLIGACAO', ''),
        (33, 'CD_NACIONALIDADE', ''),
        (34, 'DS_NACIONALIDADE', ''),
        (35, 'SG_UF_NASCIMENTO', ''),
        (36, 'CD_MUNICIPIO_NASCIMENTO', ''),
        (37, 'NM_MUNICIPIO_NASCIMENTO', ''),
        (38, 'DT_NASCIMENTO', ''),
        (39, 'NR_IDADE_DATA_POSSE', ''),
        (40, 'NR_TITULO_ELEITORAL_CANDIDATO', ''),
        (41, 'CD_GENERO', ''),
        (42, 'DS_GENERO', ''),
        (43, 'CD_GRAU_INSTRUCAO', ''),
        (44, 'DS_GRAU_INSTRUCAO', ''),
        (45, 'CD_ESTADO_CIVIL', ''),
        (46, 'DS_ESTADO_CIVIL', ''),
        (47, 'CD_COR_RACA', ''),
        (48, 'DS_COR_RACA', ''),
        (49, 'CD_OCUPACAO', ''),
        (50, 'DS_OCUPACAO', ''),
        (51, 'NR_DESPESA_MAX_CAMPANHA', ''),
        (52, 'CD_SIT_TOT_TURNO', ''),
        (53, 'DS_SIT_TOT_TURNO', ''),
        (54, 'ST_REELEICAO', ''),
        (55, 'ST_DECLARAR_BENS', ''),
        (56, 'NR_PROTOCOLO_CANDIDATURA', ''),
        (57, 'NR_PROCESSO', ''),
    ]

    relevant = [
        (2, 'ANO_ELEICAO', 'Eleição_Ano', 'integer'),
        (5, 'NR_TURNO', 'Eleição_Turno', 'integer'),
        (8, 'DT_ELEICAO', 'Eleição_Data', 'date'),
        (14, 'DS_CARGO', 'Eleição_Cargo', 'string'),
        (10, 'SG_UF', 'Eleição_UF', 'string'),
        (11, 'SG_UE', 'Eleição_UE', 'string'),
        (15, 'SQ_CANDIDATO', 'id', 'string'),
        (16, 'NR_CANDIDATO', 'Candidato_Número', 'string'),
        (17, 'NM_CANDIDATO', 'Candidato_Nome', 'string'),
        (18, 'NM_URNA_CANDIDATO', 'Candidato_NomeUrna', 'string'),
        (19, 'NM_SOCIAL_CANDIDATO', 'Candidato_NomeSocial', 'string'),
        (27, 'NR_PARTIDO', 'Grupo_Partido', 'string'),
        (30, 'SQ_COLIGACAO', 'Grupo_ColigaçãoID', 'string'),
        (32, 'DS_COMPOSICAO_COLIGACAO', 'Grupo_Coligação', 'string'),
        (35, 'SG_UF_NASCIMENTO', 'Demografia_Nascimento_UF', 'string'),
        (37, 'NM_MUNICIPIO_NASCIMENTO', 'Demografia_Nascimento_Município', 'string'),
        (38, 'DT_NASCIMENTO', 'Demografia_Nascimento_Data', 'date'),
        (39, 'NR_IDADE_DATA_POSSE', 'Demografia_Idade', 'integer'),
        (42, 'DS_GENERO', 'Demografia_Gênero', 'string'),
        (44, 'DS_GRAU_INSTRUCAO', 'Demografia_Instrução', 'string'),
        (46, 'DS_ESTADO_CIVIL', 'Demografia_EstadoCivil', 'string'),
        (48, 'DS_COR_RACA', 'Demografia_Cor', 'string'),
        (34, 'DS_NACIONALIDADE', 'Demografia_Nacionalidade', 'string'),
        (50, 'DS_OCUPACAO', 'Demografia_Ocupação', 'string'),
        (23, 'DS_SITUACAO_CANDIDATURA', 'Situação_Candidatura', 'string'),
        (25, 'DS_DETALHE_SITUACAO_CAND', 'Situação_Detalhe', 'string'),
        (53, 'DS_SIT_TOT_TURNO', 'Situação_Resultado', 'string'),
        (54, 'ST_REELEICAO', 'Situação_Reeleição', 'boolean'),
        (55, 'ST_DECLARAR_BENS', 'Situação_DeclaraçãoBens', 'boolean'),
        (20, 'NR_CPF_CANDIDATO', 'Documento_CPF', 'string'),
        (40, 'NR_TITULO_ELEITORAL_CANDIDATO', 'Documento_TítuloEleitoral', 'string'),
        (21, 'NM_EMAIL', 'Documento_email', 'string'),
        (57, 'NR_PROCESSO', 'Documento_Processo', 'string'),
    ]

    return parse_general(list_files()['candidatos'], relevant)



def parse_vagas():
    columns_all = [
        (0, 'DT_GERACAO', ''),
        (1, 'HH_GERACAO', ''),
        (2, 'ANO_ELEICAO', ''),
        (3, 'CD_TIPO_ELEICAO', ''),
        (4, 'NM_TIPO_ELEICAO', ''),
        (5, 'CD_ELEICAO', ''),
        (6, 'DS_ELEICAO', ''),
        (7, 'DT_ELEICAO', ''),
        (8, 'DT_POSSE', ''),
        (9, 'SG_UF', ''),
        (10, 'SG_UE', ''),
        (10, 'NM_UE', ''),
        (12, 'CD_CARGO', ''),
        (13, 'DS_CARGO', ''),
        (14, 'QT_VAGAS', ''),
    ]

    relevant = [
        (2, 'ANO_ELEICAO', 'Eleição_Ano', 'integer'),
        (7, 'DT_ELEICAO', 'Eleição_Data', 'date'),
        (8, 'DT_POSSE', 'Eleição_Posse', 'date'),
        (9, 'SG_UF', 'Eleição_UF', 'string'),
        (10, 'SG_UE', 'Eleição_UE', 'string'),
        (13, 'DS_CARGO', 'Eleição_Cargo', 'string'),
        (14, 'QT_VAGAS', 'Eleição_Vagas', 'integer'),
    ]

    return parse_general(list_files()['vagas'], relevant)


def parse_cassacao():
    columns_all = [
        (0, 'DT_GERACAO', ''),
        (1, 'HH_GERACAO', ''),
        (2, 'ANO_ELEICAO', ''),
        (3, 'CD_TIPO_ELEICAO', ''),
        (4, 'NM_TIPO_ELEICAO', ''),
        (5, 'CD_ELEICAO', ''),
        (6, 'DS_ELEICAO', ''),
        (7, 'SG_UF', ''),
        (8, 'SG_UE', ''),
        (9, 'NM_UE', ''),
        (10, 'SQ_CANDIDATO', ''),
        (11, 'DS_MOTIVO_CASSACAO', ''),
    ]

    relevant = [
        (2, 'ANO_ELEICAO', 'Eleição_Ano', 'integer'),
        (7, 'SG_UF', 'Eleição_UF', 'string'),
        (8, 'SG_UE', 'Eleição_UE', 'string'),
        (10, 'SQ_CANDIDATO', 'id', 'string'),
        (11, 'DS_MOTIVO_CASSACAO', 'Cassação', 'string'),
    ]

    return parse_general(list_files()['cassacao'], relevant)

def parse_bens():
    columns_all = [
        (0, 'DT_GERACAO', '', ''),
        (1, 'HH_GERACAO', '', ''),
        (2, 'ANO_ELEICAO', '', ''),
        (3, 'CD_TIPO_ELEICAO', '', ''),
        (4, 'NM_TIPO_ELEICAO', '', ''),
        (5, 'CD_ELEICAO', '', ''),
        (6, 'DS_ELEICAO', '', ''),
        (7, 'DT_ELEICAO', '', ''),
        (8, 'SG_UF', '', ''),
        (9, 'SG_UE', '', ''),
        (10, 'NM_UE', '', ''),
        (11, 'SQ_CANDIDATO', '', ''),
        (12, 'NR_ORDEM_CANDIDATO', '', ''),
        (13, 'CD_TIPO_BEM_CANDIDATO', '', ''),
        (14, 'DS_TIPO_BEM_CANDIDATO', '', ''),
        (15, 'DS_BEM_CANDIDATO', '', ''),
        (16, 'VR_BEM_CANDIDATO', '', ''),
        (17, 'DT_ULTIMA_ATUALIZACAO', '', ''),
        (18, 'HH_ULTIMA_ATUALIZACAO', '', ''),
    ]

    relevant = [
        (2, 'ANO_ELEICAO', 'Eleição_Ano', 'integer'),
        (8, 'SG_UF', 'Eleição_UF', 'string'),
        (9, 'SG_UE', 'Eleição_UE', 'string'),
        (11, 'SQ_CANDIDATO', 'id', 'string'),
        (12, 'NR_ORDEM_CANDIDATO', 'Bem_id', 'string'),
        (14, 'DS_TIPO_BEM_CANDIDATO', 'Bem_Tipo', 'string'),
        (15, 'DS_BEM_CANDIDATO', 'Bem_Detalhe', 'string'),
        (16, 'VR_BEM_CANDIDATO', 'Bem_Valor', 'float'),
        (17, 'DT_ULTIMA_ATUALIZACAO', 'Bem_Atualizado', 'date'),
    ]

    return parse_general(list_files()['bens'], relevant)


def main():
    aux = parse_candidatos()
    df_cand = pandas.DataFrame(aux[1], columns=aux[0])
    aux = parse_cassacao()
    df_cassacao = pandas.DataFrame(aux[1], columns=aux[0])
    df_cand = df_cand.merge(
        df_cassacao[['id', 'Cassação']],
        how='left',
        on='id',
    )
    aux = parse_bens()
    df_bens = pandas.DataFrame(aux[1], columns=aux[0])
    aux = parse_vagas()
    df_vagas = pandas.DataFrame(aux[1], columns=aux[0])
    return {
        'candidatos': df_cand,
        'bens': df_bens,
        'vagas': df_vagas,
    }
