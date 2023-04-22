from __future__ import annotations
from dataclasses import dataclass,field
from typing import Optional


@dataclass(slots=True)
class Key:
    term : bytes = field(default = b'')
    data_pointers : Optional[tuple[int,int]] = field(default_factory=tuple)

@dataclass(slots=True)
class CachedNode:
    key_quantity : int = field(default=0)
    #leaf : bool = True
    keys : list[Key] = field(default_factory=list)
    pointers : list[tuple[int,int,]] | list[CachedNode] = field(default_factory=list)
    
    
class CachedTree:
    def __init__(self):
    
        self.root = CachedNode()
        
    def insertNode(self,key_quantity : int , terms:list[bytes] ,node_pointers : list[tuple[int,int]],\
        data_pointers : list[tuple[int,int]],node_where_is_to_insert : CachedNode = None):
        if node_where_is_to_insert is None:
            node_where_is_to_insert = self.root
        #if node_where_is_to_insert.leaf:
        if node_where_is_to_insert.keys == []:
            node_where_is_to_insert.keys = [Key(terms[_],data_pointers[_]) for _ in range(key_quantity)]
            node_where_is_to_insert.pointers = node_pointers
            node_where_is_to_insert.key_quantity = key_quantity
            return
            
        segment_number = node_where_is_to_insert.key_quantity-1
        while(segment_number>=0):
            if node_where_is_to_insert.keys[segment_number].term < terms[0]:
                break
            segment_number-=1
        segment_number+=1
        if isinstance(node_where_is_to_insert.pointers[segment_number],CachedNode):
            self.insertNode(key_quantity,terms,node_pointers,data_pointers,node_where_is_to_insert.pointers[segment_number])
        else:
            tmp_keys = [Key(terms[_],data_pointers[_]) for _ in range(key_quantity)]
            node_where_is_to_insert.pointers[segment_number] = CachedNode(key_quantity,keys = tmp_keys,pointers = node_pointers)
            

        
    '''
        Will return tuple[str,int,int], where is str equal either 'd' or 't', int_s equal to offset and length in interesting file respectively.
        'd' means data_pointer, hence, should just read record from index file. That situation can be happend when particular term already in cachedTree.
        't' means tree_file , should read record from specific tree file and continue searching from that node.
        Third case of returning value can happend when tree is empty. 
    ''' 
    
    def searchTerm(self,term : bytes,node_where_is_search : Optional[CachedNode] = None) -> Optional[tuple[str,tuple[int,int]]]:
        if node_where_is_search is None:
            node_where_is_search = self.root
            if node_where_is_search.key_quantity==0:
                return None
        
        segment_number = node_where_is_search.key_quantity-1
        while(segment_number>=0):
            if node_where_is_search.keys[segment_number].term < term:
                break
            if node_where_is_search.keys[segment_number].term == term:
            
                return ('d',node_where_is_search.keys[segment_number].data_pointers)
            segment_number-=1
       
        
        
        segment_number+=1 
        
        if isinstance(node_where_is_search.pointers[segment_number],CachedNode):
            return self.searchTerm(term,node_where_is_search.pointers[segment_number])
        return ('t',node_where_is_search.pointers[segment_number])
        
        
            
    
    