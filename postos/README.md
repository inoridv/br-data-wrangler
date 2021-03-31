# Crawler de Postos da ANP
O crawler foi desenvolvido e estruturado como projeto, pensando em organização e reutilização na medida do possível.  

O racional para as requisições de busca dos dados e paginação foi desenvolvido a partir de uma análise da página do portal da ANP, interceptando as requisições realizadas ao submeter o formulário pelo navegador, e também analisado os arquivos e estrutura da página.

## Testes e Detecção de Erros
riscos por ex na busca pela string, etc.

modularização facilita os testes

## Data Quality
Esse projeto tem espaço para que sejam usados algumas métricas de data quality que são bastante interessantes para acompanhamento da saúde das execuções. O arquivo/pacote com métricas de data quality seria gerado fim da execução, com algumas métricas e valores relevantes para avaliação da execução realizada. Apesar de não ter sido implementado, seria simplesmente um dicionário com algumas métricas. Exemplos de boas métricas para esse projeto são:
- Países e cidades disponíveis (para acompanhar facilmente se com o tempo as opções mudaram)
- Bandeiras disponíveis (mesmo caso acima)
- Data de execução
- Duração da execução
- Informações sobre erros, se houverem.

## Comentários

## Ambiente de Execução Validado
O projeto foi executado em ambiente com:  
- Python 3.8
- Pacotes inclusos no `requirements.txt`