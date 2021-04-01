# Abordagem com PySpark
Nesta pasta o processamento foi feito utilizando o framework Apache Spark para processamento dos dados com ferramentas mais otimizadas de processamento paralelo, particionamento e agregação de dados. __O código tem algumas limitações por ter sido executado com o PySpark local sem nem uma instalação standalone de hadoop local, como por exemplo não ser capaz de ler múltiplos arquivos de uma vez nem realizar a escrita utilizando as ferramentas do Spark. Com uma instalação de Hadoop, seja local ou em cluster, torna-se possível de usar essas ferramentas normalmente. Pensando nisso, houveram algumas adaptações.__ De qualquer forma, podemos ainda aproveitar de algum paralelismo especialmente durante o processamento, mesmo localmente. 

A adaptação principal é que o Arquivo `result.csv` foi e está sendo gerado a partir do arquivo `csv_exemplo.csv`, que é uma união de 15000 linhas de cada um dos arquivos das contas ativas, para servir como exemplo da eficácia da agregação. Isso foi feito para que possa-se observar um resultado final interessante mesmo com a limitação de não ter um ambiente hadoop local para processar os múltiplos arquivos da forma ideal (um loop seria desperdício nesse caso). Foram feitos testes com arquivos grandes, como o de SP, também com sucesso. Para validar basta ingerir qualquer um deles trocando o parâmetro de path. No cenário ideal bastaria apontar a leitura para a pasta com todos os arquivos (ativos e inativos).  

Vale ressaltar que o código foi organizado e desenvolvido com um viés de script de processamento de fato, onde foram priorizadas saídas analíticas legíveis e processamento matricial ao invés de códigos mais genéricos e reaproveitamento de algumas variáveis. Algumas organizações e ordem de processamentos foram decididos pensando em otimizar o código para o processamento e _lazy loading_ em memória disponíveis no spark.  

## Notebook
No repo, além do script standalone `pyspark_processor.py`, também foi incluido o notebook `pyspark.ipynb` que foi utilizado para teste e acompanhamento, e também a versão em html `pyspark.html` para facilitar a visualização sem necessidade de executar o notebook. O código no script final pyspark_processor por consequência está mais organizado, visto que foi desenvolvido em seguida dos testes.

## Testes/Data Quality
A abordagem para testes nesse tipo de demanda fica focada na geração de um arquivo/pacote de data quality ao fim da execução, com algumas métricas e valores relevantes para avaliação da execução realizada. Apesar de não ter sido implementado, seria simplesmente um dicionário com algumas métricas que poderiam inclusive ser revisitadas com o tempo. Exemplos de boas métricas para esse script são:
- Número de linhas e partições lidas
- Quantidade de arquivos disponíveis
- Data de execução
- Duração da execução   
- Possívelmente também seria interessante guardar uma versão Raw do DataFrame agregado dos arquivos. 
- Quantidade de entradas nulas por coluna
- Métricas estatísticas descritivas _(df.describe())_ 
- Informações sobre erros, se houverem.

Para esse tipo de processamento, assume-se um cenário com uma ferramenta de orquestração cuidando do agendamento e disparo dos processamentos, o que permitira abstrair o monitoramento e notificação de falhas para a organização de dependências e execuções nessa ferramenta de orquestração. Com isso, basta buscarmos deixar os erros e logs bem descritos para debug. A ferramenta de orquestração também poderia ser responsável por disparar a análise do arquivo de data quality criado, notificando então partes interessadas em caso "suspeitos".

## Pontos de Atenção
- O Result.csv exemplo nesta pasta foi gerado a partir de um csv de exemplo incluso no repo, conforme descrito previamente no overview. Assume-se que, por termos os arquivos na máquina, na execução final, com suporte ao processamento simultâneo dos arquivos, juntaremos a base de ativos e inativos na mesma pasta para a execução do script.
- Há limitações devido à interações que precisam de ferramentas hadoop instaladas, conforme descrito previamente.
- No teste em uma máquina com i7, 16gb RAM, o processamento com os params preenchidos conforme no script estava como uma performance de aproximadamente __80MBs__ processados por segundo.
- Dados com valores faltando estão sendo descartados, mas essa abordagem é variável pra cada cenário.
- Dados não utilizados são sempre _removidos_ o quanto antes dos dataframes para otimizar o processamento.
- Mais otimizações poderiam ser feitas em um ambiente Hadoop, como alteração do parâmetro _shuffle partitions_ do spark para otimizar o paritcionamento dos dados, além de outros parâmetros de uso de memória.
- Otimizações de código também sempre devem ser consideradas, como uso de _cache()_ em DFs recorrentemente utilizados, e talvez até mesmo _casting_ do campo TP_SEXO para char/booleano, visando ocupar menos espaço na memória.


## Ambiente de Execução Validado
O Script foi executado em ambiente com:  
- Python 3.8
- Pacotes inclusos no `requirements.txt`