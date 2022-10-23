import pandas as pd
import numpy as np
import re
import time
from IPython.display import clear_output
import requests
from bs4 import BeautifulSoup


class CrawlerVerif:
    #the dataframe that will be returned after executing the crawler and will contain all crawled data
    output_columns = ['SIREN', 'Nom du compte',  'Annee', "Chiffre d'affaires", 
                         "Charges d'exploitation", 'Bénéfice ou Perte', 'Fonds propres',
                         'Immobilisations nettes', 'Trésorerie', 'Dettes financières',
                         'Total bilan', 'Frais financiers', 'Produits financiers', 
                         'Salaires et charges',  "Nombre d'employés"]
    output_df = pd.DataFrame(columns=output_columns)
    
    def __init__(self, companies_names, companies_ids):
        self.companies_names = companies_names
        self.companies_ids = companies_ids
        
        
    
    #Methode pour lire le contenu HTML d'une pageweb pour une certaine société
    def web_page_reader(self, company_id):
        url = 'https://www.verif.com/bilans-gratuits/' + str(company_id)  #URL vers le Data Provider pour un SIREN de company
        return BeautifulSoup(requests.get(url) .text, 'html.parser')

    
    #Method to get the data of a single row of an HTML table
    def row_text_extractor(self, tr, coltag='td'): # td (data) or th (header)       
            return [td.get_text(strip=True) for td in tr.find_all(coltag)]
    
    
    #Extract the data from the Table
    def table_data_extractor(self, company_id):
    
        extracted_rows_values = self.web_page_reader(company_id).find('table')
        if extracted_rows_values is not None:
            extracted_rows_values = extracted_rows_values.find_all('tr')
            years = self.row_text_extractor(extracted_rows_values[0], 'th')
            extracted_rows_values = extracted_rows_values[1:-1] #Remove some unwanted rows of the Table (e.g. the row of buying buttons)
        
            data_rows = []
            for row in extracted_rows_values: # for every table row
                data_rows.append(self.row_text_extractor(row, 'td') ) # data row
        else:
            return [], []  #empty lists  years, data_rows
        return years, data_rows


    def extracted_data_cleaning(self, years, data_rows ):
        #Select the indices of the not empty years so we can sync the years with data_rows
        indices_of_years_having_values = [ enum[0] for enum in enumerate(years) if enum[1] ]  #enum=(index, year_value) and we return a list of indices of the years that are not empty
    
        #Some cleaning of the years list
        years = [ years[index] for index in indices_of_years_having_values ]
        years = [ str(year) for year in years ]
    
        #Sync the data_rows with the not empty years values using their indices
        #then do some minor cleanings
        data_rows = [ [row[i] for i in indices_of_years_having_values] for row in data_rows ]
        data_rows = [ [int(number.replace(' ', '')) if number else np.nan for number in row ] for row in data_rows ]
    
        return years, data_rows


    #Return a concatenated list of the Data
    #To append it later to our crawler DataFrame
    def data_concatenator(self, company_name, company_id, years, data_rows):
        return [[company_id for year in years], [company_name for year in years], years] + [row for row in data_rows]


    def crawl_over_all_companies(self):
        i = 0
        for company_name, company_siren in zip(self.companies_names, self.companies_ids):
        
            clear_output(wait=True)
            #Clearing the view of the process using simple prints
            i += 1
            print(f'#{i}  Company: {company_name}')
            
            start_time = time.time()
        
            #Extracting data for our company from the website
            years, data_rows = self.table_data_extractor(company_siren)
        
            #On test si le crawler a trouvé des donnée sur la company
            if years:
                print('Crawling Task Succeeded!')
                years, data_rows = self.extracted_data_cleaning(years, data_rows)
                company_data = self.data_concatenator(company_name, company_siren, years, data_rows)  #Concatenated list of data to append to the dataframe
                #Mapping the resulted list of Data with the List of Columns to append it as a DataFrame
                self.output_df = self.output_df.append(pd.DataFrame( {self.output_columns[i]:company_data[i] for i in range(len(company_data))} ), ignore_index = True)
            #Dans le cas échéant on remplie juste les deux colonnes nom+Siren pour la société actuelle
            #Pour dire que rien est trouvé pour cette company
            else:
                print('No Found Data')
                # company_data = [company_siren, company_name]
                # Mapping the resulted list of Data with the List of Columns to append it as a DataFrame
                self.output_df = self.output_df.append({'SIREN':company_siren, 'Nom du compte':company_name}, ignore_index = True)
  
        
            print("Execution Time: %s seconds \n" % (time.time() - start_time) )
        print('Stealing\'s well done!')