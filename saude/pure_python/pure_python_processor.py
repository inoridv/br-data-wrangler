import pandas as pd
import logging
import numpy as np
from datetime import datetime
from functools import reduce
from os import listdir

############### General Setup ###############

# Desabilita warnings com falsos positivos relacionados com a manipulacao de dados usada
# Discussao vem de issues da propria lib pandas
# https://github.com/pandas-dev/pandas/pull/5390
# https://github.com/pandas-dev/pandas/issues/5597
pd.options.mode.chained_assignment = None  # default='warn'

# Nivel de log personalizado entre para logar as mensagens no codigo
# mas evitar mensagens em excesso das libs
LOG_LEVEL_NAME = "SCRIPT_INFO"
LOG_LEVEL = 35

logging.addLevelName(LOG_LEVEL, LOG_LEVEL_NAME)

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=LOG_LEVEL,
    datefmt='%Y-%m-%d %H:%M:%S')

############### Helper vars and functions ###############

today = datetime.today()
def age_from_birthdate(birthdate):
    """
    Calculates age from birthdate. Uses 'today' global variable.
    Does not consider special cases such as Feb 29th births and such.
    """
    global today

    birth = datetime.strptime(birthdate, "%d/%m/%Y")
    age = today.year - birth.year - ((today.month, today.day) < (birth.month, birth.day))
    
    return age

# Mapeamento para as regioes para otimizar o acesso posterior.
map_uf_regions = {
    "AC": "Norte",
    "AL": "Nordeste",
    "AP": "Norte",
    "AM": "Norte",
    "BA": "Nordeste",
    "CE": "Nordeste",
    "DF": "Centro-Oeste",
    "ES": "Sudeste",
    "GO": "Centro-Oeste",
    "MA": "Nordeste",
    "MT": "Centro-Oeste",
    "MS": "Centro-Oeste",
    "MG": "Sudeste",
    "PA": "Norte",
    "PB": "Nordeste",
    "PR": "Sul",
    "PE": "Nordeste",
    "PI": "Nordeste",
    "RJ": "Sudeste",
    "RN": "Nordeste",
    "RS": "Sul",
    "RO": "Norte",
    "RR": "Norte",
    "SC": "Sul",
    "SP": "Sudeste",
    "SE": "Nordeste",
    "TO": "Norte"
}

# Dataframe para agregar os processamentos em chunks dos arquivos
aggregated_files = pd.DataFrame(columns=["TP_SEXO", "REGIAO", "FAIXA_ETARIA", "QTD_PESSOAS"])

############### Execution Config ###############

conf = {
    "chunk_size": 20000,
    "files_encoding": "latin",
    # Assume-se que tanto os dados dos ativos quanto dos inativos foram adicionados na mesma pasta
    "files_location": "C:/Users/daviv/Downloads/sib_ativos",
    "files_csv_separator": ";"
}

for conf_name in conf:
    logging.log(LOG_LEVEL, "Using %s: %s" % (conf_name, conf[conf_name]))

############### Processing ###############

logging.log(LOG_LEVEL, "Starting process.")

for file in listdir(conf["files_location"]):
    logging.log(LOG_LEVEL, "Processing File %s" % file)

    for data_fragment in pd.read_csv("%s/%s" % (conf["files_location"], file), 
                                     chunksize=conf["chunk_size"], 
                                     sep=conf["files_csv_separator"], 
                                     encoding=conf["files_encoding"]):
        
        # Remove entradas sem data e/ou sexo
        processed_fragment = data_fragment.dropna(subset=["DT_NASCIMENTO", "TP_SEXO"])
        # Calcula idade
        processed_fragment["IDADE"] = processed_fragment.apply(
            lambda row: age_from_birthdate(row["DT_NASCIMENTO"]), 
            axis=1
        )

        # Mapeamento de idades para strings de faixa etaria usando o mapa auxliar map_age_ranges
        # A ordem das condicoes corresponder com a faixa etaria desejada no map_age_ranges
        map_age_condition = [
            (processed_fragment['IDADE'] <= 18), 
            (processed_fragment['IDADE'] > 18) & (processed_fragment['IDADE'] <= 45), 
            (processed_fragment['IDADE'] > 45) & (processed_fragment['IDADE'] <= 60),
            (processed_fragment['IDADE'] > 60) & (processed_fragment['IDADE'] <= 80),
            (True) #default case (>80)
        ]
        map_age_ranges = [
            "18-",
            "19-45",
            "46-60",
            "61-80",
            "81+" 
        ]
        processed_fragment['FAIXA_ETARIA'] = np.select(map_age_condition, map_age_ranges)

        # Mapeamento de UFs para regioes
        processed_fragment['REGIAO'] = processed_fragment["SG_UF"]
        processed_fragment.replace({"REGIAO": map_uf_regions}, inplace=True)

        # Agrega o numero de entradas, agrupadas por sexo/faixa etaria/regiao, na nova coluna QTD_PESSOAS
        processed_fragment = (
            processed_fragment.groupby(["TP_SEXO", "FAIXA_ETARIA", "REGIAO"])
                              .size()
                              .to_frame(name="QTD_PESSOAS")
                              .reset_index()
        )

        # Agrega o fragmento processado ao df responsavel por agregar todas as entradas da base
        aggregated_files = (
            aggregate.append(processed_fragment)
                     .groupby(["TP_SEXO", "FAIXA_ETARIA", "REGIAO"])
                     .sum()
                     .reset_index()
        )

    logging.log(LOG_LEVEL, "Finished processing File")

logging.log(LOG_LEVEL, "Finished processing all files")

# O aggregated_files ja tem a segregacao por faixa
logging.log(LOG_LEVEL, aggregated_files)

############### Final Aggregation and Consolidation ###############

logging.log(LOG_LEVEL, "Starting age range aggregation")

# Regra para agregar faixas anteriores
aggregated_age_ranges = [
    aggregated_files.loc[aggregated_files["FAIXA_ETARIA"] == "18-"]
                    .groupby(["TP_SEXO", "REGIAO"], as_index=False)
                    .agg(QTD_ATE_18=("QTD_PESSOAS", "sum")),
    aggregated_files.loc[aggregated_files["FAIXA_ETARIA"].isin(["18-", "19-45"])]
                    .groupby(["TP_SEXO", "REGIAO"], as_index=False)
                    .agg(QTD_ATE_45=("QTD_PESSOAS", "sum")),
    aggregated_files.loc[aggregated_files["FAIXA_ETARIA"].isin(["18-", "19-45", "46-60"])]
                    .groupby(["TP_SEXO", "REGIAO"], as_index=False)
                    .agg(QTD_ATE_60=("QTD_PESSOAS", "sum")),
    aggregated_files.loc[aggregated_files["FAIXA_ETARIA"].isin(["18-", "19-45", "46-60", "61-80"])]
                    .groupby(["TP_SEXO", "REGIAO"], as_index=False)
                    .agg(QTD_ATE_80=("QTD_PESSOAS", "sum")),
    aggregated_files.loc[aggregated_files["FAIXA_ETARIA"] == "81+"]
                    .groupby(["TP_SEXO", "REGIAO"], as_index=False)
                    .agg(QTD_MAIOR_80=("QTD_PESSOAS", "sum"))
]

logging.log(LOG_LEVEL, "Finished aggregation")

# Consolida os dados em um dataset final
logging.log(LOG_LEVEL, "Consolidating aggregated data")

consolidated_age_ranges = reduce(
    lambda  left,right: pd.merge(left,right,on=['TP_SEXO', 'REGIAO'], how='outer'), 
    aggregated_age_ranges
)

logging.log(LOG_LEVEL, "Finished consolidation")

logging.log(LOG_LEVEL, "Saving output file result.csv")
consolidated_age_ranges.to_csv("result.csv")
logging.log(LOG_LEVEL, "Done!")