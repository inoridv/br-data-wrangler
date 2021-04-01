# Abordagem com Python "Puro"
Por questões de preço alto de recursos em cluster, dependendo da necessidade não imediata dos dados, processar os dados com Python puro ao invés de um cluster Spark ou derivados ainda pode ser viável. Nesta pasta o processamento foi feito com isso em mente, usando apenas operações matriciais com pandas e numpy nos dados. Não seria a abordagem ideal para esse cenário, mas é viável.  

A chave para esse processamento ser possível dessa forma é os arquivos csv serem lidos com o parâmetro `chunksize`, que implica na leitura sendo feita e carregada em memória em batches de linhas, de acordo com esse param. Foi usado um valor simbólico, e ele sempre pode ser otimizado baseado nos recursos disponíveis.  

Vale ressaltar que o código foi organizado e desenvolvido com um viés de script de processamento de fato, onde foram priorizadas saídas analíticas legíveis e processamento matricial ao invés de códigos mais genéricos e reaproveitamento de algumas variáveis.  

O arquivo `result.csv` nessa pasta foi criado agregando apenas os arquivos de usuários ativos.

## Testes/Data Quality
A abordagem para testes nesse tipo de demanda fica focada na geração de um arquivo/pacote de data quality ao fim da execução, com algumas métricas e valores relevantes para avaliação da execução realizada. Apesar de não ter sido implementado, seria simplesmente um dicionário com algumas métricas que poderiam inclusive ser revisitadas com o tempo. Exemplos de boas métricas para esse script são:
- Número de linhas em cada arquivo lido
- Quantidade de arquivos lidos
- Data de execução
- Duração da execução   
- Possívelmente também seria interessante guardar uma versão Raw do DataFrame agregado dos arquivos.  
- Informações sobre erros, se houverem.

Em questão da detecção de erros, nesse caso o mais interessante seria a adição da interceptação de exceções em alguns pontos-chave como a leitura dos arquivos, e as operações de agregação e transformação. Os blocos `try except` não foram adicionados para preservar a legibilidade neste teste, mas seriam extremamente necessários em um cenário real, especialmente para permitir o envio de notificações e também permitir a geração de um pacote de data quality com valores indesejados, para que o mesmo fosse identificado por algum sistema de monitoramento.

Para esse tipo de processamento, seria também interessante ter uma ferramenta de orquestração cuidando do agendamento e disparo dos processamentos, o que permitira abstrair o monitoramento e notificação de falhas para a organização de dependências e execuções nessa ferramenta de orquestração. Com isso, basta buscarmos deixar os erros e logs bem descritos para debug. A ferramenta de orquestração também poderia ser responsável por disparar a análise do arquivo de data quality criado, notificando então partes interessadas em caso "suspeitos".

## Pontos de Atenção
- O Result.csv exemplo nesta pasta foi gerado apenas da base de usuarios ativos como exemplo. Assume-se que, por termos os arquivos na máquina, juntaremos a base de ativos e inativos na mesma pasta para a execução do script.
- No teste em uma máquina com i7, 16gb RAM, o processamento com os params preenchidos conforme no script estava como uma performance de aproximadamente 6.5MBs processados por segundo.
- Dados com valores faltando estão sendo descartados, mas essa abordagem é variável pra cada cenário.
- Melhorias poderiam ser feitas com a mesma abordagem usada na versão pyspark, gerando colunas binárias para as faixas, e usando funções de agregação do pandas para calcular a idade, ao invés de usar o .apply, que é pouco performático.

## Ambiente de Execução Validado
O Script foi executado em ambiente com:  
- Python 3.8
- Pacotes inclusos no `requirements.txt`