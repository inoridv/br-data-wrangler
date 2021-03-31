from bs4 import BeautifulSoup
import re
import logging

class PageParserANP(object):
    def __init__(self):
        self.logger = logging.getLogger('crawler_ANP')

    def get_available_brands(self, raw_page_content):
        parsed_page = BeautifulSoup(raw_page_content, 'html.parser')
        
        brands_tag = parsed_page.find(attrs={"name" : "sBandeira"})

        brands = {}

        for brand_tag in brands_tag.findChildren():
            brands[str(brand_tag.string)] = {"search_value": brand_tag["value"]}
            
        brands.pop("None") # Remove opcao Generica
        
        return brands

    def get_available_locations(self, raw_page_content):
        parsed_page = BeautifulSoup(raw_page_content, 'html.parser')

        states_tag = parsed_page.find(attrs={"name" : "sEstado"})

        locations = {}

        for state_tag in states_tag.findChildren():
            locations[str(state_tag.string)] = {"search_value": state_tag["value"], "cities": []}
            
        locations.pop("None") # Remove opcao Generica

        return locations

    def get_brand_search_results(self, raw_page_content):
        brand_search = BeautifulSoup(raw_page_content, 'html.parser')
        
        try:
            raw_results_number = brand_search.find("b", string=re.compile('.*registros.*'))

            if raw_results_number is None:
                raise Exception("No results for brand")
            
            raw_results_number = raw_results_number.string.split()[3]
            
            if not raw_results_number.isnumeric():
                raise Exception("No number found for brand, found '{}' insead" % raw_results_number)
        
        except TypeError as ex:
            logging.info ("No results for brand")

            return 0
        except Exception as ex:
            logging.info(ex)
            # Pode ter outro tipo de excecao tb
            # Mantem 0, nao achou
            return 0
    
        results_amount = int(raw_results_number)

        return results_amount

    def get_state_search_results(self, raw_page_content):
        state_search = BeautifulSoup(raw_page_content, 'html.parser')
        
        try:            
            raw_results_number = state_search.find("b", string=re.compile('.*registros.*'))
            
            if raw_results_number is None:
                raise Exception("No results for state")
            
            raw_results_number = raw_results_number.string.split()[3]
            
            if not raw_results_number.isnumeric():
                # Problema
                raise Exception("No number found, {}" % raw_results_number)
        
        except TypeError as ex:
            logging.info ("No results for state")

            return 0
        except Exception as ex:
            logging.info(ex)
            # Pode ter outro tipo de excecao tb
            # Mantem 0, nao achou
            return 0
        
        results_amount =  int(raw_results_number)
        
        return results_amount
        
    def get_state_available_cities(self, raw_page_content):
        state_search = BeautifulSoup(raw_page_content, 'html.parser')
        
        try:
            # Guarda a lista de municipios
            cities_tag = state_search.find(attrs={"name" : "sMunicipio"})
            cities = [(city_tag.string, city_tag["value"]) for city_tag in cities_tag.findChildren() if city_tag["value"] != "0"]
        
        except Exception as ex:
            logging.info(ex)
            # Pode ter outro tipo de excecao tb
            # Mantem 0, nao achou
            return 0
        
        return cities

    def get_city_search_results(self, raw_page_content):
        city_search = BeautifulSoup(raw_page_content, 'html.parser')

        try:
            raw_results_number = city_search.find("b", string=re.compile('.*registros.*'))

            if raw_results_number is None:
                raise Exception("No results for city")

            raw_results_number = raw_results_number.string.split()[3]

            if not raw_results_number.isnumeric():
                # Problema
                raise Exception("No number found, {}" % raw_results_number)

            results_amount = int(raw_results_number)

        except TypeError as ex:
            logging.info ("No results for city")

            return 0
        except Exception as ex:
            logging.info(ex)
            # Pode ter outro tipo de excecao tb
            # Mantem 0, nao achou
            # talvez tenha q fazer isso p td
            # se bem q o resto eu inicializo c 0
            return 0

        return results_amount