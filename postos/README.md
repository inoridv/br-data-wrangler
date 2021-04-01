# Scraper de Postos da ANP
O scraper foi desenvolvido e estruturado como projeto, pensando em organização e reutilização na medida do possível. Com a execução iniciando a partir do `__main__.py`, é posível executar o código diretamente a partir da pasta do módulo, com `python scraper`.  

O racional para as requisições de busca dos dados e paginação foi desenvolvido a partir de uma análise da página do portal da ANP, interceptando as requisições realizadas ao submeter o formulário pelo navegador, e também analisado os arquivos e estrutura da página. Com isso foi possível entender como interpretar e motnar requisições diretamente, e como lidar com a paginação caso fosse necessária (para por exemplo extrair nomes e derivados de cada um dos postos em um critério de busca).  

## Testes e Detecção de Erros
O código foi feito com alguns tratamentos de exceção estratégicos para otimizar o logging e facilitar a detecção de pontos de falhas e direcionamentos da execução. Também foi segregado e reorganizado para facilitar a adição de testes. Não foram adicionados por tempo hábil, mas uma suíte que cobrisse as unidades e tarefas principais de cada módulo, como a geração do arquivo final na classe `CSVSink`, a busca pelo conteúdo na classe `PageParserANP` e a agregação dos resultados na classe `ScraperANP`, tornariam ainda mais rápido o debug e manutenção em casos de falha.  

Inclusive, seria possível aproveitar os pontos de exceção destacados e disparar notificações para interessados no status da execução. Uma ferramenta de orquestração também seria de grande relevância, podendo detectar a não execução do fluxo dentro de um horário limite e outros cenários de falha, permitindo atuação sem atraso.

## Data Quality
Esse projeto tem espaço para que sejam usados algumas métricas de data quality que são bastante interessantes para acompanhamento da saúde das execuções. O arquivo/pacote com métricas de data quality seria gerado fim da execução, com algumas métricas e valores relevantes para avaliação da execução realizada. Apesar de não ter sido implementado, seria simplesmente um dicionário com algumas métricas. Exemplos de boas métricas para esse projeto são:
- Países e cidades disponíveis (para acompanhar facilmente se com o tempo as opções mudaram)
- Bandeiras disponíveis (mesmo caso acima)
- Data de execução
- Duração da execução
- Informações sobre erros, se houverem.

## Pontos de Atenção
- Pensando em um cenário de execução recorrente desse scraper, seria interessante executar o crawling de bandeiras em paralelo ao de estados e municípios, para acelerar o processo. Há a possibilidade de melhorias com bibliotecas de processamento paralelo também.
- Apesar do projeto estar segregado em classes, claramente o código ainda tem bastante acoplamento. A segregação foi feita principalmente para aumentar a manutenibilidade e evitar classes grandes, além de criar mais pontos unitários onde testes automatizados poderiam ser aplicados individualmente. Considerando isso, entretanto, é possível se pensar na possibilidade de desenvolvimento de um scraper/biblioteca de crawling generica. É difícil que tal atenda a todos os cenários, mas poderia acelerar muito o processo em vários casos.
- Há alguns riscos na abordagem usada para encontrar a tag com o valor de retorno da busca, que é o alvo do scraping. Apesar do risco de mudanças ser intrínseco à scrapers, seria mais seguro chegar até a tag utilizando o caminho das tags no HTML, para em seguida buscar por um valor numérico dentro da string, ao invés da abordagem atual que busca com regex por um valor direto na string, esperando uma string específica, para então retirar o valor numérico de uma posição X. A abordagem atual é mais passível à falhas com mudanças pequenas, mas foi utilizada por ser prática dado o tempo disponível.
- Dependendo do tipo de acompanhamento, os logs podem/devem ser reduzidos, focando nos casos de falha.
- Foram priorizadas otimizações e desvios de requisições desnecessárias, o que melhora a performance mas reduz um pouco a padronização do código.

## Ambiente de Execução Validado
O projeto foi executado em ambiente com:  
- Python 3.8
- Pacotes inclusos no `requirements.txt`