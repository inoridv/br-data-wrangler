# Abordagem com Python "Puro"
Por questões de preço de recursos, dependendo da necessidade não imediata dos dados, processar os dados com Python puro ao invés de um cluster Spark ou derivados ainda pode ser viável. Nesta pasta o processamento foi feito com isso em mente, usando apenas operações matriciais com pandas e numpy nos dados.  

A chave para esse processamento ser possível dessa forma é os arquivos csv serem lidos com o parâmetro `chunksize`, que implica na leitura sendo feita e carregada em memória em batches de linhas, de acordo com esse param. Foi usado um valor simbólico, e ele sempre pode ser otimizado baseado nos recursos disponíveis.

## Testes/Data Quality
A abordagem para testes nesse tipo de demanda fica focada na geração de um arquivo/pacote de data quality ao fim da execução, com algumas métricas e valores relevantes para avaliação da execução realizada. Apesar de não ter sido implementado, seria simplesmente um dicionário com algumas métricas. Exemplos de boas métricas para esse script são:
- Número de linhas em cada arquivo lido
- Quantidade de arquivos lidos
- Data de execução
- Duração da execução   
- Possívelmente também seria interessante guardar uma versão Raw do DataFrame agregado dos arquivos.  
- Informações sobre erros, se houverem.

Em questão da detecção de erros, nesse caso o mais interessante seria a adição da interceptação de exceções em alguns pontos-chave como a leitura dos arquivos, e as operações de agregação e transformação. Os blocos `try except` não foram adicionados para preservar a legibilidade neste teste, mas seriam extremamente necessários em um cenário real, especialmente para permitir o envio de notificações e também permitir a geração de um pacote de data quality com valores indesejados, para que o mesmo fosse identificado por algum sistema de monitoramento.

## Comentários
- O Result.csv exemplo nesta pasta foi gerado apenas da base de usuarios ativos como exemplo. Assume-se que, por termos os arquivos na máquina, juntaremos a base de ativos e inativos na mesma pasta para a execução do script.
- No teste em uma máquina com i7, 16gb RAM, o processamento com os params preenchidos conforme no script estava como uma performance de aproximadamente 6.5MBs processados por segundo.
- O código foi organizado e feito com um viés de script de processamento de fato, onde foram priorizadas saídas analíticas legíveis e processamento matricial, ao invés de códigos mais genéricos e reaproveitamento de algumas variáveis.

## Ambiente de Execução Validado
O Script foi executado em ambiente com:  
- Python 3.8
- Pacotes inclusos no `requirements.txt`