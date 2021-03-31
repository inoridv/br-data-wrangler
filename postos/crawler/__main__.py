from crawler import CrawlerANP
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

crawler = CrawlerANP()
crawler.crawl()