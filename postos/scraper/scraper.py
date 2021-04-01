import requests
import logging
import sys
import re
from urllib.parse import urlencode
from sink import CSVSink
from page_parser import PageParserANP

class ScraperANP(object):
    def __init__(self):
        # Objetos auxiliares
        self.logger = logging.getLogger('scraper_ANP')
        self.parser = PageParserANP()
        self.sink = CSVSink()

        # Configurações para as requisições ao portal
        self.base_url = "https://postos.anp.gov.br/consulta.asp"
        self.base_request_params = {
            "sCnpj": "",
            "sRazaoSocial": "",
            "sEstado": 0,
            "sMunicipio": 0,
            "sBandeira": 0,
            "sProduto": 0,
            "sTipodePosto": 0,
            "p": "1",
            "hPesquisar": "PESQUISAR"
        }
        self.base_request_headers = {
            "Content-Type": "application/x-www-form-urlencoded",
        }
        self.PARAM_BANDEIRA = "sBandeira"
        self.PARAM_ESTADO = "sEstado"
        self.PARAM_MUNICIPIO = "sMunicipio"

    def scrape(self, scrape_targets=["states", "top_cities", "relevant_brands"], relevant_brands=[]):
        """
        Realiza o scraping do portal ANP. Funcao de entrada de todo o processo.
        Parametros modularizam qual tipo de informacao deve ser buscada.
        """
        if len(scrape_targets) == 0:
            self.logger.error("No scrape choice provided")
            sys.exit(1)

        self.logger.info("Scraper starting..")

        # Busca inicialmente quais estados e bandeiras estao disponiveis no portal
        # Formato atual dessa chamada nao esta adequada com o parametro scrape_targets
        available_states, available_brands = self.scrape_available_states_and_brands()

        # Faz o scraping dos dados por estado, ja aproveitando as requisicoes
        # para guardar as cidades disponiveis em cada estado.
        # Top cities depende de states, entao tambem causa a execucao dessa parte
        if "states" in scrape_targets or "top_cities" in scrape_targets:
            
            self.logger.info("Scraping state info")
            states_info, available_cities = self.scrape_states_info(available_states)
            self.logger.info("Done")

            self.sink.dispatch(states_info, "postos_por_estado.csv", ['Estado','Postos'])


        # Faz o scraping por cidade e gera as top 100
        if "top_cities" in scrape_targets:

            self.logger.info("Scraping top cities info")
            top_cities_info = self.scrape_top_cities_info(available_cities)
            self.logger.info("Done")
            
            self.sink.dispatch(top_cities_info, "postos_por_municipio.csv", ['Municipio','Postos'])


        # Faz o scraping por bandeira considerando apenas as relevantes
        if "relevant_brands" in scrape_targets:

            self.logger.info("Scraping relevant brands info")
            brands_info = self.scrape_brands_info(available_brands, relevant_brands)
            self.logger.info("Done")

            self.sink.dispatch(brands_info, "postos_por_bandeira.csv", ['Bandeira','Postos'])

    def scrape_available_states_and_brands(self):
        """
        Retorna quais estados e bandeiras estao disponiveis no portal a partir de uma
        requisicao simples.
        """
        raw_base_page = requests.get(self.base_url).text

        self.logger.info("Scraping available states")
        available_states = self.parser.get_available_items(raw_base_page, {"name" : self.PARAM_ESTADO})
        self.logger.info("Done")

        self.logger.info("Scraping available brands")
        available_brands = self.parser.get_available_items(raw_base_page, {"name" : self.PARAM_BANDEIRA})
        self.logger.info("Done")

        return available_states, available_brands   

    def scrape_brands_info(self, brands, relevant_brands):
        """
        Faz o scraping para todas as bandeiras disponiveis e retorna uma lista de tuplas
        contendo a quantidade de postos para cada bandeira, considerando apenas as bandeiras
        relevantes, e o resto como 'OUTRA'
        """
        request_params = self.base_request_params
        request_headers = self.base_request_headers

        # Inicializa a entrada OUTRA para as bandeiras nao relevantes
        relevant_brands_info = {"OUTRA": 0}

        for brand in brands:
            request_params[self.PARAM_BANDEIRA] = brands[brand]["search_value"]
            
            self.logger.info("Scraping info for brand %s" % brand)
            
            form_data = urlencode(request_params)
            raw_brand_search = requests.post(self.base_url, headers=request_headers, data=form_data).text

            brand_results_amount = self.parser.get_search_results(
                raw_brand_search, 
                find_tag="b", 
                find_string=re.compile('.*registros.*'),
                item_name="brand"
            )

            # Guarda a quantidade de postos da marca se for uma das relevantes
            if brand in relevant_brands:
                relevant_brands_info[brand] = brand_results_amount
            else:
                relevant_brands_info["OUTRA"] += brand_results_amount

        print(tuple(relevant_brands_info.items()))
        return tuple(relevant_brands_info.items())

    def scrape_states_info(self, states):
        """
        Faz o scraping para todos os estados disponiveis e retorna uma lista de tuplas
        contendo a quantidade de postos por estado.
        A funcao tambem ja obtem e retorna a lista de municipios disponiveis por estado
        para possiveis usos futuros.
        """
        request_params = self.base_request_params
        request_headers = self.base_request_headers
        
        states_info = []
        available_cities = {}

        for state in states:
            request_params[self.PARAM_ESTADO] = states[state]["search_value"]

            self.logger.info("Scraping info for state %s" % state)

            form_data = urlencode(request_params)
            raw_state_search = requests.post(self.base_url, headers=request_headers, data=form_data).text

            # Guarda as cidades disponiveis para o estado
            self.logger.info("Scraping state's available cities")
            available_cities[state] = self.parser.get_available_items(raw_state_search, {"name" : self.PARAM_MUNICIPIO})

            # Guarda a quantidade de postos no estado
            state_results = self.parser.get_search_results(
                raw_state_search, 
                find_tag="b", 
                find_string=re.compile('.*registros.*'),
                item_name="state"
            )
            states_info.append((state, state_results))

        return states_info, available_cities

    def scrape_top_cities_info(self, states_city_map):
        """
        Faz o scraping para todas as cidades disponiveis em cada estado e retorna uma lista de tuplas
        contendo as 100 cidades com mais postos.
        """
        request_params = self.base_request_params
        request_headers = self.base_request_headers

        top_cities = []

        # Para cada estado
        for state in states_city_map:
            request_params[self.PARAM_ESTADO] = state

            # Para cada cidade
            for city in states_city_map[state]:

                request_params[self.PARAM_MUNICIPIO] = states_city_map[state][city]["search_value"]
                logging.info("Scraping info for city %s - %s" % (city, state))

                form_data = urlencode(request_params)
                raw_city_search = requests.post(self.base_url, headers=request_headers, data=form_data).text
                
                city_results = self.parser.get_search_results(
                    raw_city_search,
                    find_tag="b", 
                    find_string=re.compile('.*registros.*'),
                    item_name="brand"
                )

                # Se ainda nao temos 100 cidades agregadas, apenas adiciona a tupla na lista
                # Caso contrario, substitui a cidade com a menor quantidade de postos pela cidade
                # Atual, se a atual tiver mais postos. Com isso mantemos os top 100 sem precisar
                # Reordenar a lista com frequencia
                if len(top_cities) < 100:
                    top_cities.append((city, city_results))
                else:
                    # Obtem a tupla da cidade com menor numero de postos
                    min_city = min(top_cities, key = lambda t: t[1])
                    # Se a cidade atual possuir mais postos que a cidade com o menor numero
                    # substitui ela pela atual
                    if city_results > min_city[1]:
                        top_cities.remove(min_city)
                        top_cities.append((city, city_results))
        
        # Ordena a lista de forma decrescente baseado no numero de postos.
        top_cities.sort(reverse=True, key = lambda t: t[1])
        
        return top_cities