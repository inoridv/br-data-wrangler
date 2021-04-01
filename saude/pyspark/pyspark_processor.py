from pyspark.sql import SparkSession
from itertools import chain
import pyspark.sql.functions as F

# Usado somente para processamento em cluster Hadoop
# from pyspark import SparkContext
# sc = SparkContext()

# Exemplo log estivessemos em um ambiente hadoop
# log4jLogger = sc._jvm.org.apache.log4j
# LOGGER = log4jLogger.LogManager.getLogger(__name__)
# LOGGER.info("pyspark script logger initialized")

############### General Setup ###############

spark = SparkSession.builder \
                    .appName("Processador Planos de Saude") \
                    .getOrCreate()
# Removido pois so sera util em notebooks
# .config("spark.sql.repl.eagerEval.enabled", True) \


############### Helper vars and functions ###############
CSV_DELIMITER = ","
ENCODING = "ISO-8859-1"

DATE_FORMAT = "dd/MM/yyyy"

# Colunas necessarias dos dados raw
RELEVANT_COLUMNS = ["TP_SEXO", "DT_NASCIMENTO", "SG_UF"]

# Mapeamento para as regioes para otimizar o acesso posterior.
MAP_UF_REGIONS = {
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

############### Processing ###############

# InferSchema implica em uma segunda leitura dos dados, e nem sempre é uma boa escolha
dataset = spark.read.options(
        delimiter=CSV_DELIMITER, 
        header="True", 
        encoding=ENCODING, #latin
        dateFormat=DATE_FORMAT,
        inferSchema="True"
    ).csv("csv_exemplo.csv") # 1 ou mais arquivos.. Ideal seria ler todos, depende de particionamento e etc do Hive

# Remove colunas desnecessarias
unnecessary_columns = [column for column in dataset.schema.names if column not in RELEVANT_COLUMNS]
clean_dataset = dataset.drop(*tuple(unnecessary_columns))

# Cast da coluna DT_NASCIMENTO para Date e calculo de idade a partir do nascimento
clean_dataset = clean_dataset.withColumn("DT_NASCIMENTO", F.to_date(F.col("DT_NASCIMENTO"), DATE_FORMAT)) \
                             .withColumn(
                                 "IDADE", 
                                 F.floor(
                                     F.datediff(
                                         F.current_date(), 
                                         F.to_date(F.col("DT_NASCIMENTO"), DATE_FORMAT)
                                     )/365.25
                                 )
                             ) \
                             .drop("DT_NASCIMENTO")

# Criação de colunas para cada faixa etaria com valores 0/1
clean_dataset = clean_dataset.withColumn("ATE_18", 
                                         F.when(F.col("IDADE") <= F.lit(18), F.lit(1))
                                         .otherwise(F.lit(0))
                                        ) \
                             .withColumn("ATE_45", 
                                         F.when(F.col("IDADE") <= F.lit(45), F.lit(1))
                                         .otherwise(F.lit(0))
                                         ) \
                             .withColumn("ATE_60", 
                                         F.when(F.col("IDADE") <= F.lit(60), F.lit(1))
                                         .otherwise(F.lit(0))
                                         ) \
                             .withColumn("ATE_80", 
                                         F.when(F.col("IDADE") <= F.lit(80), F.lit(1))
                                         .otherwise(F.lit(0))
                                         ) \
                             .withColumn("MAIOR_80", 
                                         F.when(F.col("IDADE") > F.lit(80), F.lit(1))
                                         .otherwise(F.lit(0))
                                         ) \
                             .drop("IDADE")

# Mapeamento de UF para regiao
region_mapping = F.create_map([F.lit(x) for x in chain(*MAP_UF_REGIONS.items())])

clean_dataset = clean_dataset.withColumn("REGIAO", region_mapping[clean_dataset['SG_UF']]) \
                             .drop("SG_UF")


############### Final Aggregation and Consolidation ###############

# Agrupa pelas colunas relevantes e faz o aggreate somando os valores nos campos de faixa etaria.
# Pela estrategia de colunas 1/0, basta somar a coluna como um todo.
processed_data = clean_dataset.groupby("TP_SEXO", "REGIAO").agg(
    F.sum("ATE_18").alias("ATE_18"),
    F.sum("ATE_45").alias("ATE_45"),
    F.sum("ATE_60").alias("ATE_60"),
    F.sum("ATE_80").alias("ATE_80"),
    F.sum("MAIOR_80").alias("MAIOR_80")
).orderBy("REGIAO")


# Escreve o dataset final com os dados desejados.
# Acaba nao sendo possivel utilizar o write distribuido do spark pela falta do ambiente Hadoop.
#processed_data.write.csv("result.csv")
processed_data.toPandas().to_csv("result.csv")

