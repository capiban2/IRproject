from btreeeffort3 import TraverseThroughTree
import multiprocessing
from encodeASCIIletters import encodeString
from typing import Optional
import os
from typing import Generator,Iterable


PATHTOMAPPING='/home/iv/Documents/mydir/kursovaya/resultindexstorage/mapping.txt'
PATHTOINDEX='/home/iv/Documents/mydir/kursovaya/resultindexstorage/index.bin'
PATHTOTREESTORAGE='/home/iv/Documents/mydir/kursovaya/treestorage'


'''
    Method returns list of pairs, which contains term(string) and either posting list,correspoding to that term, or None,
    means, what that term doesnt contain in collection.
'''

def useTraverse(pathtodir : str, pathtoindex : str, terms_union : list[bytes]) -> list[list[int]]:
    
    tmp = TraverseThroughTree(pathtodir,pathtoindex,terms_union)
    return tmp.traverse()


'''
    Initial value of terms in that stage is list of string, because this function
    will be using when there will be a need search particular words in all collection.
    
    Encode string func works like hash-func and in the same time can save some storage.
'''
def search(terms : list[str],pathtotreedir : str = PATHTOTREESTORAGE,pathtoindex : str = PATHTOINDEX) -> list[Optional[list[int]]]:
    
    
    mapping = {}
    encoded_terms = (encodeString(_) if _.isalpha() else _.encode('utf-8') for _ in terms)
    
    for encoded_term in encoded_terms:
        if len(encoded_term) not in mapping.keys():
            mapping[len(encoded_term)] = [encoded_term]
            continue
        mapping[len(encoded_term)].append(encoded_term)
    
    workers_quantity = os.cpu_count() if len(mapping.keys())>os.cpu_count() else len(mapping.keys())
    if workers_quantity==1:
        return useTraverse(pathtotreedir,pathtoindex,list(mapping.values())[0])
    with multiprocessing.Pool(processes=workers_quantity) as mp:
        search_result = mp.starmap(
           useTraverse,
           ((pathtotreedir,pathtoindex,term_union) for term_union in mapping.values())
            
        )
    flatten = []
    for process_result in search_result:
        flatten.extend(process_result)
    return flatten
    
    

'''
    Returning value is list of tuples, each pair contains one document's name ,from collection,
    and boolean value, means, whether there is this pool of terms in specific document or not.
    
    Posting orders documents from 1, not from zero.
    
    For now func will return only documents, which contain all terms.
'''
def BooleanRetreival(terms : list[str])->list[str]:
    with open(PATHTOMAPPING) as mapping_file:
        doc_names = mapping_file.read().strip(' ').split(' ')
    if len(result_search :=search(terms)) > 1:
        result_search = intersectionLists(result_search)
    else:
        result_search = result_search[0] 
    
    #result_of_search = intersectionLists(search(terms))
    return [doc_names[_-1] for _ in result_search]
    
def intersectionLists(posting : list[list[int]])->list[int]:
    intersect = _intersectionTwoLists(posting[0],posting[1])
    for postlist in posting[2:]:
        intersect = _intersectionTwoLists(postlist,intersect)
    return intersect
    
    

def _intersectionTwoLists(f_list : list[int],s_list:list[int] | Generator[int,None,None])->Generator[int,None,None]:
    
    return (_ for _ in set(s_list) if _ in f_list)

# def flatten(items,ignore_type = (int,)):
#     for x in items:
#         if isinstance(x,Iterable):
#             yield from 


if __name__ =='__main__':
    # a = [1,2,3,4,5,6]
    # b = (_ for _ in range(0,8,2))
    # intersect = list(_intersectionTwoLists(a,b))
    
    # posting = [[_ for _ in range(10)], [_ for _ in range(1,11,2)]+[2], [_ for _ in range(0,10,2)]]
    # print(posting)
    # res = list(intersectionLists(posting))
    # assert list(intersectionLists([[1,2,3],[2,3,4],[0,1,2]])) == [2]
    # assert list(intersectionLists([[1,2,3],[1,2,3],[1,2,3],[1,2,3]])) == [1,2,3]
    # assert list(intersectionLists([[1,2,3],[4,5,6],[7,8,9]])) == []
    res = BooleanRetreival(['0','0'])
    pass
    
    
    