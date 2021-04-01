from scraper import ScraperANP
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

# Poderia pegar o scrape_choices do terminal, pra ajudar a executar rapidamente
# coisas diferentes. Foi parametrizado para facilitar executar em paralelo
# Top_cities é dependente de states
scraper = ScraperANP()
scraper.scrape(
    scrape_targets=["states", "top_cities", "relevant_brands"], 
    relevant_brands=["RAIZEN", "BRANCA", "IPIRANGA", "PETROBRAS DISTRIBUIDORA S.A."]
)