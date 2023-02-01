import requests
import json
import os
import datetime
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from urllib3 import Retry
from queue import Queue
from threading import Thread
import time
import copy




class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]             
            del kwargs["timeout"]
        else:
            self.timeout = 5   # or whatever default you want
        super().__init__(*args, **kwargs)
    def send(self, request, **kwargs):
        kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)
    

SAVE_PARSE_PROGRESS_PATH = "parse_progress"

if not os.path.exists(SAVE_PARSE_PROGRESS_PATH):
    os.makedirs(SAVE_PARSE_PROGRESS_PATH)

# need to create file with empty dict or have an existing one in case of crash
GLOBAL_DICT = json.load(open(os.path.join(SAVE_PARSE_PROGRESS_PATH, 'current_adj_dict.json'), 'r'))
PREV_PROGRESS = len(GLOBAL_DICT)
SAVE_SUCCESS_EVERY = 100
SAVE_FAILS_EVERY = 50

# need to create file with empty dict or have an existing one in case of crash
FAILED_ENTITIES = json.load(open(os.path.join(SAVE_PARSE_PROGRESS_PATH, 'known_failed_entities.json'), 'r'))
KNOWN_FAILS_SET = set([pair[0] for pair in FAILED_ENTITIES])
    
    
REQUESTS_ERRORS_SET = set(['timeout', 'connection_error', 'retry_error', 'json_decode_error', 'bad_responce'])
SESSION = requests.Session()
REQUESTS_ADAPTER = TimeoutHTTPAdapter(timeout=(3, 10), 
                                      max_retries=Retry(total=3, backoff_factor=3, 
                                                        allowed_methods=False, 
                                                        status_forcelist=[429, 500, 502, 503, 504]))
SESSION.mount("http://", REQUESTS_ADAPTER)
SESSION.mount("https://", REQUESTS_ADAPTER)
WIKIDATA_URL = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql'

CURR_RUN_FILENAME = 'all_entities.json'
curr_run_entities = json.load(open(CURR_RUN_FILENAME, 'r'))

entities_list = curr_run_entities[:80]




def preformat_request_input(query):
    full_query = f"""
    PREFIX wikibase: <http://wikiba.se/ontology#>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    {query}
    """
    return full_query


def request_wikidata(query):
    result = None
    responce = None
    try:
        responce = SESSION.get(WIKIDATA_URL, 
                               params={'query': query, 'format': 'json'})
    except (requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout):
        result = 'timeout'
    except requests.exceptions.ConnectionError:
        result = 'connection_error'
    except (requests.exceptions.RetryError, requests.exceptions.ConnectionError):
        result = 'retry_error'

    if responce is not None and responce.ok:
        try:
            result = responce.json()
        except json.decoder.JSONDecodeError:
            result = 'json_decode_error'
    else:
        result = 'bad_responce'
    return result

def create_sparql(entity_id, entity_place):
    result_query = None
    if entity_place == 'subj':
        result_query = f"SELECT DISTINCT ?p WHERE {{{entity_id} ?p  ?x.}}"
    elif entity_place == 'obj':
        result_query = f"SELECT DISTINCT ?p WHERE {{?x ?p  {entity_id}.}}"
    return result_query

def run_request(entity_id):
    subj_sparql = create_sparql(entity_id, 'subj')
    obj_sparql = create_sparql(entity_id, 'obj')
    
    result = {'subj': None, 'obj': None}
    
    preformatted_subj_sparql = preformat_request_input(subj_sparql)
    preformatted_obj_sparql = preformat_request_input(obj_sparql)

    requests_subj_result = request_wikidata(preformatted_subj_sparql)
    requests_obj_result = request_wikidata(preformatted_obj_sparql)

    result['subj'] = requests_subj_result
    result['obj'] = requests_obj_result
    
    return result

def parse_result(responce_payload):
    preds_list = []
    if 'results' in responce_payload:
        if 'bindings' in responce_payload['results']:
            for binding in responce_payload['results']['bindings']:
                preds_list += [binding['p']['value']]
    preds_set = list(set(preds_list))
    return preds_set

def check_responce_for_errors(adj_dict_sample):
    subj_check = True if type(adj_dict_sample['subj']) == str and adj_dict_sample['subj'] in REQUESTS_ERRORS_SET else False
    obj_check = True if type(adj_dict_sample['obj']) == str and adj_dict_sample['obj'] in REQUESTS_ERRORS_SET else False
    
    return subj_check & obj_check


def get_entity_predicates(entity_id):
    if entity_id not in GLOBAL_DICT and entity_id not in KNOWN_FAILS_SET:
        request = run_request(entity_id)
        check_responce_for_errors_status = check_responce_for_errors(request)
        if check_responce_for_errors_status == False:
            clean_result = dict()
            clean_result['subj'] = parse_result(request['subj'])
            clean_result['obj'] = parse_result(request['obj'])

            # if we need to treat empty predicates as error - sometimes wikidata API failes to return them
            # if len(clean_result['subj']) > 0 and len(clean_result['obj']) > 0:
            GLOBAL_DICT[entity_id] = clean_result
            # else:
            #     FAILED_ENTITIES.append([entity_id, 'zero_in_preds'])
        else:
            FAILED_ENTITIES.append([entity_id, 'request_error'])


class PredicateInfoWorker(Thread):

    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        with tqdm(total=len(entities_list)) as pbar:
            while True:
                entity_id = self.queue.get()
                try:
                    get_entity_predicates(entity_id)
                finally:
                    self.queue.task_done()
                    
                    if len(GLOBAL_DICT) % SAVE_SUCCESS_EVERY == 0:
                        global_dict_copy = copy.deepcopy(GLOBAL_DICT)
                        print(f'Curr size = {len(GLOBAL_DICT)}')
                        json.dump(global_dict_copy,
                                  open(os.path.join(SAVE_PARSE_PROGRESS_PATH, f"adj_dict_vol{RUN_NUMBER}_percent{round(len(GLOBAL_DICT) / len(entities_list), 2)}_{CURR_DATE}.json"), 'w'),
                                  ensure_ascii=False, indent=4)

                    if len(FAILED_ENTITIES) % SAVE_FAILS_EVERY == 0:
                        failed_entities_copy = copy.deepcopy(FAILED_ENTITIES)
                        json.dump(failed_entities_copy,
                                  open(os.path.join(SAVE_PARSE_PROGRESS_PATH, f"fails_vol{RUN_NUMBER}_percent{round(len(FAILED_ENTITIES) / len(entities_list), 2)}_{CURR_DATE}.json"), 'w'),
                                  ensure_ascii=False, indent=4)
                        
                    pbar.update(1)
                    


if __name__ == "__main__":
    
        
    RUN_NUMBER = 3
    WORKER_NUM = 8
    
    CURR_DATE = datetime.datetime.today().strftime('%Y-%m-%d')
    
    
    queue = Queue()

    try:
        for x in range(WORKER_NUM):
            worker = PredicateInfoWorker(queue)
            worker.daemon = True
            worker.start()

        for entity_id in entities_list:
            queue.put(entity_id)
        queue.join()
            
    except KeyboardInterrupt:
        print('Stopped iteration!')
            
    current_global_dict = json.load(open('parse_progress/current_adj_dict.json', 'r'))
    if len(GLOBAL_DICT) > len(current_global_dict):
        json.dump(GLOBAL_DICT, open('parse_progress/current_adj_dict.json', 'w'), ensure_ascii=False, indent=4)
        print(f'Previous parse size was {len(current_global_dict)}, now {len(GLOBAL_DICT)}')
        
        
    current_know_fails = json.load(open(os.path.join(SAVE_PARSE_PROGRESS_PATH, 'known_failed_entities.json'), 'r'))
    if len(FAILED_ENTITIES) > len(current_know_fails):
        json.dump(FAILED_ENTITIES, 
                  open(os.path.join(SAVE_PARSE_PROGRESS_PATH, f"known_failed_entities.json"), 'w'), 
                  ensure_ascii=False, indent=4)
        print(f'Previous failed entities size was {len(current_global_dict)}, now {len(GLOBAL_DICT)}')
            
    print('Done parsing!')
    
                
                
    
        
            
            
            
            
            



