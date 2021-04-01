from bs4 import BeautifulSoup
import logging

class PageParserANP(object):
    def __init__(self):
        self.logger = logging.getLogger('scraper_ANP')

    def get_available_items(self, raw_page_content, find_attrs):
        """
        Retorna os itens e valor de busca de tags do tipo 'option' disponiveis
        em uma tag 'select' que deve ser capaz de ser encontrada com o filtro find_attrs
        """
        parsed_page = BeautifulSoup(raw_page_content, 'html.parser')
        
        items_tag = parsed_page.find(attrs=find_attrs)

        items = {}

        if items_tag == None:
            self.logger.warn("No available item found using filter")
            return items

        for item_tag in items_tag.findChildren():
            if item_tag["value"] == "0":
                continue

            items[str(item_tag.string)] = {"search_value": item_tag["value"]}
        
        return items

    def get_search_results(self, raw_page_content, find_tag, find_string, item_name):
        """
        Retorna a quantidade de itens encontrados na busca no portal da ANP baseado na
        pagina de resultados. Para encontrar a quantidade, sao usados parametros de filtro
        que devem ser capazes de identificar a tag com a string de resultados
        """
        item_search = BeautifulSoup(raw_page_content, 'html.parser')
        
        try:
            # Seria mais seguro chegar até a tag utilizando o caminho das tags no HTML,
            # e em seguida buscar por um valor numérico dentro da string, ou usando o id do portal.
            raw_results_tag = item_search.find(find_tag, string=find_string)

            if raw_results_tag is None:
                raise Exception("No results for %s" % item_name)
            
            # Inseguro, conforme comentario acima.
            raw_results_number = raw_results_tag.string.split()[3]
            
            if not raw_results_number.isnumeric():
                raise Exception("No number found for %s, found '%s' instead" % (item_name, raw_results_number))
        
        except TypeError as ex:
            # Excecao causada ao tentar acessar a string split. Indica que a string
            # nao foi encontrada no estado esperado
            logging.warn(
                "Unable to find search results in result string for %s. Assuming no results available." 
                % item_name
            )

            return 0
        except Exception as ex:
            logging.warn(
                "Exception found while trying to find search results for %s: { %s }. Assuming no results available." 
                % (item_name, ex)
            )

            return 0
    
        results_amount = int(raw_results_number)

        return results_amount