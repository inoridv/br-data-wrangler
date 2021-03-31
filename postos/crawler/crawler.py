import requests
import logging
from urllib.parse import urlencode
from sink import CSVSink
from page_parser import PageParserANP

#TODO:
#- trycatches e tratamentos
# tests
# escrever q agendaria pra rodar em paralelo o estado+cidade e o brands
# rever e melhorar todos os logs
# rever estruturas de dados e cosias despadronizadas
# documentar

class CrawlerANP(object):
    def __init__(self):
        self.logger = logging.getLogger('crawler_ANP')
        self.parser = PageParserANP()
        self.sink = CSVSink()
        self.base_url = "https://postos.anp.gov.br/consulta.asp"
        self.relevant_brands = {"RAIZEN": 0, "BRANCA": 0, "IPIRANGA": 0, "PETROBRAS DISTRIBUIDORA S.A.": 0, "OUTRA": 0}
        self.locations = {}
        self.top_cities = []
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

    def crawl(self):
        raw_base_page = requests.get(self.base_url).text

        available_locations = self.parser.get_available_locations(raw_base_page)
        available_brands = self.parser.get_available_brands(raw_base_page)

        self.crawl_states_info(available_locations)
        self.sink.generate_states_csv(self.locations)
        self.crawl_cities_info(self.locations)
        self.sink.generate_cities_csv(self.top_cities)
        self.crawl_brands_info(available_brands)
        self.sink.generate_brands_csv(self.relevant_brands)
        

    def crawl_brands_info(self, brands):
        request_params = self.base_request_params
        request_headers = self.base_request_headers

        for brand in brands:
            request_params["sBandeira"] = brands[brand]["search_value"]
            
            self.logger.info("Crawling info for brand %s" % brand)
            
            form_data = urlencode(request_params)

            raw_brand_search = requests.post(self.base_url, headers=request_headers, data=form_data).text

            brand_results_amount = self.parser.get_brand_search_results(raw_brand_search)
            
            if brand in self.relevant_brands:
                self.relevant_brands[brand] = brand_results_amount
            else:
                self.relevant_brands["OUTRA"] += brand_results_amount

    def crawl_states_info(self, locations):
        request_params = self.base_request_params
        request_headers = self.base_request_headers

        for state in locations:
            request_params["sEstado"] = locations[state]["search_value"]

            self.logger.info("Crawling info for state %s" % state)

            form_data = urlencode(request_params)

            raw_state_search = requests.post(self.base_url, headers=request_headers, data=form_data).text
            
            locations[state]["cities"] = self.parser.get_state_available_cities(raw_state_search)
            
            locations[state]["amount"] = self.parser.get_state_search_results(raw_state_search)

            self.locations[state] = locations[state]

    def crawl_cities_info(self, locations):
        request_params = self.base_request_params
        request_headers = self.base_request_headers

        top_cities = []
        min_city = 0

        for state in locations:
            request_params["sEstado"] = locations[state]["search_value"]

            for city_info in locations[state]["cities"]:

                request_params["sMunicipio"] = city_info[1]
                logging.info("Crawling info for city %s - %s" % (city_info[0], state))
                form_data = urlencode(request_params)
                raw_city_search = requests.post(self.base_url, headers=request_headers, data=form_data).text
                
                city_results = self.parser.get_city_search_results(raw_city_search)

                if len(top_cities) < 100:
                    top_cities.append((city_info[0], city_results))
                else:
                    self.logger.info("Checking min")
                    min_city = min(top_cities, key = lambda t: t[1])
                    logging.info(min_city)
                    if city_results > min_city[1]:
                        self.logger.info("surpassed_min")
                        top_cities.remove(min_city)
                        top_cities.append((city_info[0], city_results))
        
        # Naive, nao tem ordenacao e resolucao de empates e etc., vai ser na orden de execucao que vai ficar
        # Min foi feito pra n fica sorteando td hr
        self.top_cities = top_cities.sort(reverse=True, key = lambda t: t[1])