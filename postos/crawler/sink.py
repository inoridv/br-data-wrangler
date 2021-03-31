import csv

class CSVSink(object):
    def generate_cities_csv(self, cities):
        with open("postos_por_cidade.csv",'w', newline="") as output:
            csv_output = csv.writer(output)
            csv_output.writerow(['Municipio','Postos'])
            for city in cities:
                csv_output.writerow(city)

    def generate_brands_csv(self, cities):
        with open('postos_por_bandeira.csv','w', newline="") as output:
            csv_output = csv.writer(output)
            csv_output.writerow(['Bandeira','Postos'])
            for city in cities:
                csv_output.writerow((brand, relevant_brands_amounts[brand]))

    def generate_states_csv(self, locations):
        with open('postos_por_estado.csv','w', newline="") as output:
            csv_output = csv.writer(output)
            csv_output.writerow(['Estado','Postos'])
            for state in locations:
                csv_output.writerow((state, locations[state]["amount"]))