import pandas as pd
import numpy as np
import math
from IPython.display import clear_output
import random

from retrying import retry
import time
import requests
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor
from itertools import compress
from functools import partial

import warnings
warnings.filterwarnings("ignore")

structures_api = "https://scanr-api.enseignementsup-recherche.gouv.fr/api/v2/structures/search/"
publications_api = "https://scanr-api.enseignementsup-recherche.gouv.fr/api/v2/publications/search"

@retry
def test_if_found(Id: str,
                  endpoint: str,
                  fields_to_return: list):
    
    """we send a post query to the API using the Id (siren, publication id, ..)"""
    
    time.sleep(0.1)
    clear_output(wait=True)
    print(f"new Id: #{Id}")
    
    headers = {"content-type": "application/json"}
    body = {"query": Id, "sourceFields": fields_to_return, "searchFields": ["id"]}
        
    with requests.Session() as session:
        request = session.post(url=endpoint, headers=headers, json=body)
        found = request.json().get("total")

    return found



@retry
def post_query_by_id(Id: str,
                   endpoint: str,
                   fields_to_return: list):
    """
    we send a post query to the API using the Id (siren, publication id, ..)
    """
    clear_output(wait=True)
    print(f"new Id: #{Id}")

    headers = {"content-type": "application/json"}
    body = {"query": Id, "sourceFields": fields_to_return, "searchFields": ["id"]}
    
    with requests.Session() as session:
        request = session.post(url=endpoint, headers=headers, json=body)
        
    results = request.json().get("results")
    # if the output of the request have values
    if results:
        results = results[0].get("value")

    return results


def filter_publication_id(data):
    """
    extracting the publications data (publication id, title, ..)
    """
    if data:
        filtered =  list(map(lambda x: x.get("publication").get("id"), data))
        return filtered
    else:
        return data


def get_companies_publications(Ids,
                               endpoint=publications_api,
                               fields_to_return=["year", "type", "title", "patents"]):
    """
    for each structure, we use the list of ids of publications
    to get their data
    """
    if Ids:
        return list(map(partial(post_query_by_id,
                                endpoint=endpoint,
                                fields_to_return=fields_to_return),
                                Ids))


def format_publication(publication):
    """
    formatting the values of a publication
    """
    output = dict()
    output["siren"] = None
    output["year"] = publication.get("year")
    output["type"] = publication.get("type")
    output["title"] = publication.get("title").get("en")  if publication.get("title").get("en") \
                                                          else publication.get("title").get("fr")
    output["n_patents"] = len(publication.get("patents")) if publication.get("patents") \
                                                          else None

    return output



def format_company_publications(publications):
    """
    do the formating for all a company"s publications
    """    
    if publications:
        output = list(map(format_publication, publications))
        return output
    else:
        output = {"siren": None,
              "year": None,
              "type": None,
              "title": None,
              "n_patents": None}
        output = [output]
        return output


def map_ids_with_data(Ids, data, field: str="siren"):
    """
    map two collections one for ids & second for data
    """
    _input = data
    output = list()
    for index, Id in enumerate(Ids):
        update_arg = {field: Id}
        for element in data[index]:
            element.update(update_arg)
            output.append(element)
    return output
