from btreeeffort3 import TraverseThroughTree
import multiprocessing
from encodeASCIIletters import encodeString
from typing import Optional
import os

def useTraverse(pathtodir : str, pathtoindex : str, terms : list[bytes]):
    tmp = TraverseThroughTree(pathtodir,pathtoindex,terms)
    return tmp.traverse()


'''
    Initial value of terms in that stage is list of string, because this function
    will be using when there will be a need search particular words in all collection.
    
    Encode string func works like hash-func and in the same time can save some storage.
'''
def search(pathtotreedir : str,pathtoindex : str, terms : list[str]) -> list[Optional[list[int]]]:
    
    pathtotreedir = pathtotreedir
    pathtoindex = pathtoindex
    mapping = {}
    encoded_terms = (encodeString(_) for _ in terms)
    
    for encoded_term in encoded_terms:
        if len(encoded_term) not in mapping.keys():
            mapping[len(encoded_term)] = [encoded_term]
            continue
        mapping[len(encoded_term)].append(encoded_term)
    
    workers_quantity = os.cpu_count() if len(mapping.keys()) else len(mapping.keys())
    with multiprocessing.Pool(processes=workers_quantity) as mp:
        search_result = mp.starmap(
           useTraverse,
           ((pathtotreedir,pathtoindex,term_union) for term_union in mapping.values())
            
        )
    return search_result 
    
    
    
        