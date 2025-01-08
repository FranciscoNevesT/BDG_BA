import os
import sqlite3
import numpy as np
import pandas as pd
import geopandas

def remove_aspas(value):
    """Removes double quotes from a string."""
    if isinstance(value, str):
        return value.replace('"', '')
    return value


def read_csv(path):

    """Reads a CSV file, removes inconsistent rows, and drops columns with only one unique value."""
    with open(path,"r") as file:
        text = file.read()

    num_campos = len(text.split("\n")[0].split(";"))
    data = []
    for line in text.split("\n"):
        campos = line.split(";")
        if len(campos) == num_campos:
            data.append(campos)

    columns = data[0]
    columns = [i[1:-1] for i in columns]
    data = data[1:]
    data = pd.DataFrame(data, columns=columns)

    # Corrigir strings
    data = data.applymap(remove_aspas)

    for col in data.columns:
        if data[col].nunique() == 1:
            data.drop(col, axis=1, inplace=True)

    for i in data.columns:
        try:
            data[i] = data[i].astype(int)
        except:
            try:
                column_values = data[i]
                column_values = column_values.apply(lambda x: x.replace(",", ".")).astype(float)
                data[i] = column_values
            except:
                pass

    return data

def corrigir_municipos_nomes(data):
    """Standardizes municipality names."""
    corrections = {
        'CAMACÃ': 'CAMACAN',
        'SANTO ESTEVÃO': 'SANTO ESTÊVÃO',
        'CAEM': 'CAÉM',
        'DIAS D ÁVILA': "DIAS D'ÁVILA",
        'Araças':'ARAÇÁS',
        'Iuiú': 'IUIU',
        'Santa Teresinha': 'SANTA TEREZINHA',
        'Muquém de São Francisco': 'MUQUÉM DO SÃO FRANCISCO'

    }
    return data.replace(corrections)

def create_table(dataframe, table_name, connection):
    """Creates a table in the SQLite database from a DataFrame."""
    dataframe.to_sql(table_name, connection, if_exists='replace', index=False)

def format_municipios(db_file):
    """Loads and formats municipality data."""
    data = geopandas.read_file("datasets/municipios/BA_Municipios_2022.dbf")

    ## municipio
    data_sql = data.drop(['NM_MUN', 'SIGLA_UF'], axis=1)
    data_sql.to_file(db_file, driver='SQLite', layer="municipio")

    return data

def format_partido(connection,mun):
    #Load data
    data = read_csv("datasets/partido/votacao_partido_munzona_2022_BA.csv")

    # Handle missing values
    data.replace(data['SG_FEDERACAO'].iloc[0], pd.NA, inplace=True)

    # Drop redundant columns
    redundant_cols = ['CD_ELEICAO', 'DT_ELEICAO', 'NR_FEDERACAO', 'CD_CARGO']
    data.drop(columns=redundant_cols, inplace=True)


    # Corrigir municipios nomes
    data = corrigir_municipos_nomes(data)
    mun_dict = {nm_mun.upper(): cd_mun for cd_mun, nm_mun in mun[['CD_MUN', 'NM_MUN']].values}
    data['CD_MUN'] = data['NM_MUNICIPIO'].apply(lambda x: mun_dict[x])
    data['NM_MUN'] = data['NM_MUNICIPIO']

    data['CD_MUN'] = data['NM_MUNICIPIO'].apply(lambda x: mun_dict[x])
    data['NM_MUN'] = data['NM_MUNICIPIO']

    data.drop(['CD_MUNICIPIO', 'NM_MUNICIPIO'], axis=1, inplace=True)

    # Create the sql tables
    create_table(data[[
        'CD_MUN', 'NR_TURNO', 'NR_ZONA', 'DS_CARGO', 'NR_PARTIDO',
        'QT_VOTOS_LEGENDA_VALIDOS', 'QT_TOTAL_VOTOS_LEG_VALIDOS',
        'QT_VOTOS_NOMINAIS_VALIDOS', 'SQ_COLIGACAO'
    ]], "votos_partido", connection)

    create_table(data[[
        'NR_PARTIDO', 'NM_PARTIDO', 'TP_AGREMIACAO', 'SG_PARTIDO'
    ]].drop_duplicates().dropna(), "partido", connection)

    create_table(data[[
        'SG_FEDERACAO', 'NM_FEDERACAO', 'DS_COMPOSICAO_FEDERACAO'
    ]].drop_duplicates().dropna(), "federacao", connection)

    create_table(data[[
        'NM_COLIGACAO', 'SQ_COLIGACAO', 'DS_COMPOSICAO_COLIGACAO'
    ]].drop_duplicates().dropna(), "coligacao", connection)

    create_table(data[[
        'CD_MUN', 'NM_MUN'
    ]].drop_duplicates().dropna(), "municipio_nome", connection)

def format_candidato(connection,mun):
    """Processes and formats candidate data."""
    #Load data
    data = read_csv("datasets/candidato/votacao_candidato_munzona_2022_BA.csv")

    # Handle missing values
    data.replace(data['SG_FEDERACAO'].iloc[0], pd.NA, inplace=True)

    # Tirar dados redundantes
    redudante = ['CD_ELEICAO', 'DT_ELEICAO', 'NR_FEDERACAO', 'CD_CARGO']
    data.drop(redudante, axis=1, inplace=True)


    # Corrigir municipios nomes
    data = corrigir_municipos_nomes(data)
    mun_dict = {nm_mun.upper(): cd_mun for cd_mun, nm_mun in mun[['CD_MUN', 'NM_MUN']].values}
    data['CD_MUN'] = data['NM_MUNICIPIO'].apply(lambda x: mun_dict[x])
    data['NM_MUN'] = data['NM_MUNICIPIO']

    data['CD_MUN'] = data['NM_MUNICIPIO'].apply(lambda x: mun_dict[x])
    data['NM_MUN'] = data['NM_MUNICIPIO']

    data.drop(['CD_MUNICIPIO', 'NM_MUNICIPIO'], axis=1, inplace=True)

    # Create the sql tables
    create_table(data[[
        'NR_TURNO', 'NR_ZONA', 'DS_CARGO', 'NR_CANDIDATO', 'NM_CANDIDATO',
        'NM_URNA_CANDIDATO', 'NM_SOCIAL_CANDIDATO', 'CD_SITUACAO_CANDIDATURA',
        'DS_SITUACAO_CANDIDATURA', 'NR_PARTIDO', 'SG_FEDERACAO', 'SQ_COLIGACAO',
        'QT_VOTOS_NOMINAIS', 'NM_TIPO_DESTINACAO_VOTOS', 'QT_VOTOS_NOMINAIS_VALIDOS',
        'CD_SIT_TOT_TURNO', 'DS_SIT_TOT_TURNO', 'CD_MUN'
    ]], "votos_candidato", connection)

def format_censo(connection):
    csv_names = ['alfabetizacao', 'basico', 'domicilio1', 'domicilio2', 'domicilio3', 'cor_raca', 'demografia',
                 'domicilio_indigena', 'domicilio_quilombolas', 'obitos', 'parentesco', 'indigenas',
                 'quilombolas']

    for csv in csv_names:
        # Load data
        path = "datasets/{}/".format(csv)
        csv_path = "{}{}".format(path,os.listdir(path)[0])

        data = read_csv(csv_path)

        data.drop('NM_MUN',axis=1,inplace=True)

        data.to_sql(csv, connection, if_exists='replace', index=False)

def format_cad_unico(connection):
    cad_unico = pd.read_csv("datasets/cad_unico/cad_unico.csv")

    cad_unico['anomes'] = cad_unico['anomes'].astype(str)
    cad_unico['ano'] = cad_unico['anomes'].str[:4]
    cad_unico['mes'] = cad_unico['anomes'].str[4:]
    cad_unico['ano'] = cad_unico['ano'].astype(int)
    cad_unico['mes'] = cad_unico['mes'].astype(int)
    cad_unico.drop('anomes', axis=1, inplace=True)

    cad_unico['CD_MUN'] = cad_unico['ibge']
    cad_unico.drop('ibge',axis=1,inplace=True)

    table_name = 'cadastro_unico'
    cad_unico.to_sql(table_name, connection, if_exists='replace', index=False)

def format_bolsa_familia(connection):
    bf_2021 = pd.read_csv("datasets/bolsa_familia/bolsa_familia.csv")

    bf_2021['anomes'] = bf_2021['anomes'].astype(str)
    bf_2021['ano'] = bf_2021['anomes'].str[:4]
    bf_2021['mes'] = bf_2021['anomes'].str[4:]
    bf_2021['ano'] = bf_2021['ano'].astype(int)
    bf_2021['mes'] = bf_2021['mes'].astype(int)
    bf_2021.drop('anomes', axis=1, inplace=True)

    bf_2021.dropna(axis=0,inplace=True)

    bf_2021['CD_MUN'] = bf_2021['ibge']
    bf_2021.drop('ibge',axis=1,inplace=True)

    table_name = 'bolsa_familia'
    bf_2021.to_sql(table_name, connection, if_exists='replace', index=False)

def format_censo_agro(connection, mun):
    gdf = geopandas.read_file('datasets/censo_agro/mun_agro/mun_agro.shp')

    gdf[['municipio', 'estado']] = gdf['MUNICIPIO'].str.split(' - ', expand=True)

    gdf = gdf[gdf['estado'] == 'BA']
    gdf.drop(['estado', 'MUNICIPIO'], axis=1, inplace=True)

    rename_dict = {
        "V1": "num_estab_mun",  # Total de estabelecimentos agropecuários, por município.
        "V2": "area_media_ha",
        "V3": "media_pessoal_ocupado_estab",
        # Média de pessoal ocupado por estabelecimento, por município, considerando-se pessoal ocupado como o total de trabalhadores com e sem laços de parentesco com o produtor.
        "V4": "area_lavoura_abud",
        # Média da área de lavouras por adubadeira ou distribuidora de calcáreo, por município, calculada pelo quociente entre a área total de lavouras e a quantidade de adubadeiras.
        "V5": "area_lavoura_colh",
        # Média da área de lavouras por colheitadeira, por município, calculada pelo quociente entre a área total de lavouras e a quantidade de colheitadeiras.
        "V6": "area_lavoura_seme",
        # Média da área de lavouras por semeadeira ou plantadeira, por município, calculada pelo quociente entre a área total de lavouras e a quantidade de semeadeiras.
        "V7": "area_lavoura_trator",
        # Média da área de lavouras por trator, por município, calculada pelo quociente entre a área total de lavouras e a quantidade de tratores.
        "V8": "lavoura_temporaria",
        # Percentual de estabelecimentos pertencentes ao Grupo de Atividade Econômica Produção de lavouras temporárias, em relação ao total de estabelecimentos agropecuários do município.
        "V9": "lavoura_permanente",
        # Percentual de estabelecimentos pertencentes ao Grupo de Atividade Econômica Produção de lavouras permanentes, em relação ao total de estabelecimentos agropecuários do município.
        "V10": "atividade_pecuaria",
        # Percentual de estabelecimentos pertencentes ao Grupo de Atividade Econômica Pecuária, em relação ao total de estabelecimentos agropecuários do município.
        "V11": "atividade_hort_flor",
        # Percentual de estabelecimentos pertencentes ao Grupo de Atividade Econômica Horticultura e floricultura, em relação ao total de estabelecimentos agropecuários do município.
        "V12": "atividade_sem_mudas",
        # Percentual de estabelecimentos pertencentes ao Grupo de Atividade Econômica Produção de sementes e mudas certificadas, em relação ao total de estabelecimentos agropecuários do município.
        "V13": "atividade_florestal",
        # Percentual de estabelecimentos pertencentes ao Grupo de Atividade Econômica Produção Florestal (florestas plantadas e florestas nativas), em relação ao total de estabelecimentos agropecuários do município.
        "V14": "atividade_pesca",
        # Percentual de estabelecimentos pertencentes ao Grupo de Atividade Econômica Pesca, em relação ao total de estabelecimentos agropecuários do município.
        "V15": "atividade_agric",
        # Percentual de estabelecimentos pertencentes ao Grupo de Atividade Econômica Aquicultura, em relação ao total de estabelecimentos agropecuários do município.
        "V16": "perc_terras_lavoura",
        # Percentual de área classificada como lavoura (temporária + permanente) no tema Utilização das Terras, em relação à área total dos estabelecimentos agropecuários do município.
        "V17": "perc_terras_pastagem",
        # Percentual de área classificada como pastagem (natural + plantada) no tema Utilização das Terras, em relação à área total dos estabelecimentos agropecuários do município.
        "V18": "perc_aves_corte",
        # Percentual de estabelecimentos cuja finalidade principal da criação de aves é Produção para corte, pintos de 1 dia ou venda de matrizes, em relação ao total de estabelecimentos agropecuários do município.
        "V19": "perc_aves_ovos",
        # Percentual de estabelecimentos cuja finalidade principal da criação de aves é Produção de ovos, em relação ao total de estabelecimentos agropecuários do município.
        "V20": "perc_bov_corte",
        # Percentual de estabelecimentos cuja finalidade principal da criação de bovinos é Corte, em relação ao total de estabelecimentos agropecuários do município.
        "V21": "perc_bov_leite",
        # Percentual de estabelecimentos cuja finalidade principal da criação de bovinos é Leite, em relação ao total de estabelecimentos agropecuários do município.
        "V22": "rendimento_arroz",
        # (kg/ha) Rendimento médio de arroz em casca, definido pelo quociente entre a produção e a área colhida, por município.
        "V23": "rendimento_cana",
        # (kg/ha) Rendimento médio de cana-de-açúcar, definido pelo quociente entre a produção e a área colhida, por município.
        "V24": "rendimento_mandioca",
        # (kg/ha) Rendimento médio de mandioca, definido pelo quociente entre a produção e a área colhida, por município.
        "V25": "rendimento_milho",
        # (kg/ha) Rendimento médio de milho em grão, definido pelo quociente entre a produção e a área colhida, por município.
        "V26": "rendimento_soja",
        # (kg/ha) Rendimento médio de soja em grão, definido pelo quociente entre a produção e a área colhida, por município.
        "V27": "rendimento_trigo",
        # (kg/ha) Rendimento médio de trigo em grão, definido pelo quociente entre a produção e a área colhida, por município.
        "V28": "rendimento_cacau",
        # (kg/ha) Rendimento médio de cacau, definido pelo quociente entre a produção e a área colhida, por município.
        "V29": "rendimento_café",
        # (kg/ha) Rendimento médio de café, definido pelo quociente entre a produção e a área colhida, por município.
        "V30": "rendimento_laranja",
        # (kg/ha) Rendimento médio de laranja, definido pelo quociente entre a produção e a área colhida, por município.
        "V31": "rendimento-uva",
        # (kg/ha) Rendimento médio de uva, definido pelo quociente entre a produção e a área colhida, por município.
        "V32": "carga_bovinos",  # (n/ha) Quantidade de bovinos (n) por área de pastagem (ha), por município.
        "V33": "cisterna",
        # (%) Percentual de estabelecimentos agropecuários com cisterna, em relação ao total de estabelecimentos no município.
        "V34": "utiliz_agrotox",
        # (%) Percentual de estabelecimentos agropecuários com declaração de uso de agrotóxicos em relação ao total de estabelecimentos agropecuários no município. A variável totaliza os que usaram agrotóxicos e os que não precisaram usar em 2016.
        "V35": "despesa_agrotox",
        # (%) Participação da despesa com agrotóxicos na despesa total do estabelecimento agropecuário, por município.
        "V36": "uso_irregação",
        # (%) Percentual de estabelecimentos agropecuários com declaração de uso de irrigação em relação ao total de estabelecimentos agropecuários no município.
        "V37": "assistencia_tec",
        # (%) Percentual de estabelecimentos agropecuários com declaração de assistência técnica em relação ao total de estabelecimentos agropecuários no município.
        "V38": "agricultura_fam",
        # (%) Percentual de estabelecimentos agropecuários classificados como Agricultura Familiar em relação ao total de estabelecimentos agropecuários no município. A definição legal de Agricultura Familiar consta no Decreto nº 9.064, de 31 de maio de 2017.
        "V39": "prod_escol_ens_fund",
        # (%) Percentual de produtores, em relação ao total no município, cujo curso escolar frequentado mais elevado corresponde, no máximo, ao Ensino Fundamental. Estão incluídos: classe de alfabetização - CA, alfabetização de jovens e adultos – AJA, antigo primário (elementar), regular do ensino fundamental ou 1º grau, educação de jovens e adultos – EJA e supletivo do ensino fundamental ou do 1º grau.
    }
    gdf.rename(columns=rename_dict, inplace=True)

    gdf = corrigir_municipos_nomes(gdf)

    mun_index = {i[1].lower(): int(i[0]) for i in mun.values}

    gdf['CD_MUN'] = gdf['municipio'].apply(lambda x: mun_index[x.lower()])

    gdf.drop(['OBJECTID','Shape_Leng','Shape_Area','geometry','municipio'],axis=1,inplace=True)
    gdf = gdf.reset_index(drop=True)

    table_name = 'censo_agro'
    gdf.to_sql(table_name, connection, if_exists='replace', index=False)

def create_db(db_path = 'sql'):
    name = 'eleicao.db'
    db_file = os.path.join(db_path, name)

    if os.path.isfile(db_file):
        os.remove(db_file)

    mun = format_municipios(db_file)

    connection = sqlite3.connect(db_file)

    format_partido(connection = connection, mun = mun)
    format_candidato(connection = connection, mun = mun)
    format_censo(connection = connection)
    format_cad_unico(connection=connection)
    format_bolsa_familia(connection=connection)
    format_censo_agro(connection=connection,mun = mun)

    connection.close()



if __name__ == '__main__':

    create_db()