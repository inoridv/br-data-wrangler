import csv
import logging

class CSVSink(object):
    def __init__(self):
        self.logger = logging.getLogger('scraper_ANP')

    def dispatch(self, rows_list, filename, headers):
        """
        Dispara os dados para a Sink. No caso dessa Sink do tipo CSV, escreve os dados
        em arquivo.
        """
        self.logger.info("Dispatching data to file %s" % filename)

        with open(filename, 'w', newline="") as output_file:
            stream_writer = csv.writer(output_file)
            stream_writer.writerow(headers)
            stream_writer.writerows(rows_list)
        
        self.logger.info("Done")
