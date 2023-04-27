from __future__ import annotations
from mergepostrings import MergePostings
from encodeASCIIletters import encodeString
from dataclasses import dataclass,field
from typing import Optional
from enum import Enum,auto
import struct
import os
import re
from itertools import zip_longest
import weakref
from encodeDigits import packedBCD,decodePackedBCD
from cachedTree import CachedTree
import multiprocessing

DICTIONARY : list[str] = [
    'run','arbitary','never',
    'can','call','typing',
    'stop','annotation',
    'classmethod','optional',
    'experiment','dataclass'
]

LENGTHDEPENDSDICTIONARY : list[bytes] = [
    b'\x15\x16\x18',
    b'\x33\x23\x1f',
    b'\xfd\x12\x54',
    b'\x43\x12\xa1',
    b'\xa1\xfd\x4a',
    b'\xad\xf4\xa5',
    b'\x1f\xa3\x11',
    b'\x55\x23\x43',
    b'\x11\x22\x33',
    b'\x22\x11\x33',
    b'\x44\x11\x33',
    b'\x55\x12\x43',
    b'\xa6\x54\xaa',
    b'\x18\x19\x20',
    b'\x1f\x2a\x3e',
    b'\xaf\x14\x1a',
    b'\xdd\xaa\xcc',
    b'\x1c\x1d\x1a'
]

'''
    this enums is using to distinguish between words of rare length and average;
    in average case will have many more words, hence there should be much more keys per level
    long or short words ,except stop-words, probability less and consequently if we have one degree for 
    both cases, in second we will have short tree with big nodes; then we can achieve significant imppvement, i guess,
    if value of keys per level will be less, then len of nodes shrink and process of seraching specific term will be take 
    less time;
    Here by length of words means essentially length of encoded term.
'''

class Range(Enum):
    LONGSHORT = auto()
    AVERAGE = auto()

DEGREEPERLEN : dict[Range,int] = {
    Range.LONGSHORT : 50,
    Range.AVERAGE : 100
}


NODETYPE : dict[bytes,str] = {
    b'l' : 'leaf',
    b'n' : 'node'
}

@dataclass(slots=True)
class Key:
    data_pointer : tuple[int,int,] = field(default_factory=tuple)
    term : bytes =field(default=b'') 
    
@dataclass(slots=True)
class Node:
    leaf:bool = True
    key_quantity:int = field(default=0)
    keys:list[Key] = field(default_factory=list)
    pointers: list[ ReferencableNode | tuple[int,int,] | None ] =field(default_factory=list)
    parrent_pointer : weakref.ProxyType= field(default = None)
    left_pointer : weakref.ProxyType= field(default = None)
    right_pointer : weakref.ProxyType= field(default = None)

class ReferencableNode(Node):
    __slots__ = ('__weakref__',)



 
    # def __del__(self):
    #    print('delete')
   

    
class FixedBtree:
    def __init__(self):
        pass


    #on this step received key looks like data_pointer and term encoded by custom encoding already. (not just utf-8 encoding)
    def insertKey(self,received_key:Key,node_where_is_insert:Optional[ReferencableNode | weakref.ProxyType] = None):
        
        if node_where_is_insert is None:
            if self.root.key_quantity == self.degree-1:
                self.__split_child()
            node_where_is_insert = self.root 
        if node_where_is_insert.leaf:
       
            node_where_is_insert.key_quantity+=1
            if not node_where_is_insert.keys or node_where_is_insert.keys[-1].term < received_key.term:
                node_where_is_insert.keys.append(received_key)
                return
            number = node_where_is_insert.key_quantity-2
            while number>=0:
                if node_where_is_insert.keys[number].term < received_key.term:
                    break
                number-=1
            number+=1
            
                
            node_where_is_insert.keys.insert(number,received_key)
            return
        
        node_number = node_where_is_insert.key_quantity-1
        while node_number>=0:
            if node_where_is_insert.keys[node_number].term < received_key.term:
                break
            node_number-=1
        # if node_number<0:
        #     node_number = 0
        node_number+=1
        if node_where_is_insert.pointers[node_number].key_quantity == self.degree-1:
            self.__split_child(node_where_is_insert,node_number)
            if node_where_is_insert.keys[node_number].term < received_key.term:
                node_number+=1 
        '''
            Probably mistake have made here.
        '''
        
        self.insertKey(received_key,node_where_is_insert.pointers[node_number])
                    

    
    def makeTree(self,degree:int = 2,keylen:int = 3):
        self.root = ReferencableNode()
        self.degree = degree
        self.tree_length = 1
        self.keylen = keylen
    def size(self):
        return 1
    
    def __split_child(self,parrent_node : Optional[ReferencableNode] = None,index_denote_to_fullchild:Optional[int]=None):
        median_position = (self.degree-1)//2
        
        fullchild_node = self.root if parrent_node is None else parrent_node.pointers[index_denote_to_fullchild]
        
        new_right_node = ReferencableNode(leaf = fullchild_node.leaf, keys = fullchild_node.keys[median_position+1:],
                            pointers=fullchild_node.pointers[median_position+1:], 
                            key_quantity=fullchild_node.key_quantity-(median_position+1),
                            parrent_pointer=weakref.proxy(parrent_node) if parrent_node is not None else None,
                            right_pointer=fullchild_node.right_pointer)
        new_left_node = ReferencableNode(leaf = fullchild_node.leaf, keys = fullchild_node.keys[:median_position],
                            pointers = fullchild_node.pointers[:median_position+1],
                            key_quantity=median_position,parrent_pointer=weakref.proxy(parrent_node) if parrent_node is not None else None,
                            left_pointer=fullchild_node.left_pointer,right_pointer=weakref.proxy(new_right_node))
        new_right_node.left_pointer = weakref.proxy(new_left_node)
        
        if parrent_node is None:
            self.root = ReferencableNode(leaf = False,pointers = [new_left_node,new_right_node],
                            keys = [fullchild_node.keys[median_position]],key_quantity=1)
            new_left_node.parrent_pointer = weakref.proxy(self.root)
            new_right_node.parrent_pointer = weakref.proxy(self.root)
            new_right_node.left_pointer = weakref.proxy(new_left_node)
            new_left_node.right_pointer = weakref.proxy(new_right_node)
            
            self.tree_length+=1
            
        else:
            parrent_node.keys.insert(index_denote_to_fullchild,fullchild_node.keys[median_position])
            parrent_node.pointers.pop(index_denote_to_fullchild)
            parrent_node.pointers.insert(index_denote_to_fullchild,new_left_node)
            parrent_node.pointers.insert(index_denote_to_fullchild+1,new_right_node)
            parrent_node.key_quantity+=1 
            if fullchild_node.left_pointer is not None:
                fullchild_node.left_pointer.right_pointer = weakref.proxy(new_left_node) 
            if fullchild_node.right_pointer is not None:
                fullchild_node.right_pointer.left_pointer = weakref.proxy(new_right_node)
                
        for node in new_right_node.pointers:
                node.parrent_pointer = weakref.proxy(new_right_node)
        for node in new_left_node.pointers:
            node.parrent_pointer = weakref.proxy(new_left_node)
    
    
    '''
        Convert tree to binary format on disk.
        Serialize tree's structure.
    ''' 
    
    def storeTree(self,pathtodir : str = '/tmp'):
        with open(os.path.join(pathtodir,f'{self.keylen}btree.bin'),'wb') as binary_output:
            file_offset = 0
            current_node = self.root
            if self.tree_length > 1:
                while not current_node.pointers[0].leaf:
                    current_node = current_node.pointers[0]
                while current_node is not None:
                    file_offset=self.__storeOneLevel(file_offset,current_node,'right',binary_output)
                    current_node = current_node.parrent_pointer
            
            encoded_root = self.__packNode(self.root) 
            # if len(encoded_root)!=((self.keylen+12)*self.root.key_quantity+9):
            #     raise RuntimeError(f'Encoded size isnt fit in expected size : ({len(encoded_root)} : {((self.keylen+13)*self.root.key_quantity+8)})') 
            binary_output.write(encoded_root + struct.pack('<IH',file_offset,len(encoded_root)))
            # length = len(encoded_root)
            self.root = ReferencableNode()
            

    def __storeOneLevel(self,current_offset,start_node : Node,direction:str,fd):
        for id,elem in enumerate(start_node.pointers):
            record = self.__packNode(elem)
            # if elem.leaf and len(record)!=((self.keylen+6)*elem.key_quantity+3):
            #     raise RuntimeError(f'Encoded size isnt fit in expected size : ({len(record)} : {((self.keylen+7)*elem.key_quantity+2)})')
            # if not elem.leaf and len(record)!=((self.keylen+12)*elem.key_quantity+9):
            #     raise RuntimeError(f'Encoded size isnt fit in expected size : ({len(record)} : {((self.keylen+13)*elem.key_quantity+8)})')
            fd.write(record)
            #del start_node.pointers[id]
            start_node.pointers[id] = [current_offset,len(record)]
            current_offset+=len(record)
        if start_node.__getattribute__(f'{direction}_pointer') is None:
            return current_offset
        else:
            return self.__storeOneLevel(current_offset,start_node.__getattribute__(f'{direction}_pointer'),direction,fd)
        
    
    '''
       
        Each node will be save by follow law:
        For instance, keylen and quantity of keys in this node is M and k respectively.
        
        1. Node is leaf
        First part for encoding contain 'l' letter, which coded by one char, one short unsigned integer, associated with quantity of keys and then
        all keys with whitespace separated writed sequentially.
        After that, the same process applying to data_pointers and these codes will be apended to the previous encoding.
        Each code of data_pointers encoding contain two digits, offset in sought file and length of sought record respectively,
        both of which are encoded as unsigned integer.
        
        3. Node is inner
        Almost all actions listed above will be the same, except in encoding of data_pointers will be appended one pointer in this file to child node,
        associated with specific key of this node.
        
        
        New rules have arise : all digits received in this process will be encoded with packedBCD, hence, their decoding 
        should invoke decodeBCD. All digits, except key_quantity ,just for simplify decoding.
        Bad idea. Discarded.

    ''' 
    
    def __packNode(self,node : Node) -> bytes:
        encoded_record = struct.pack(f'<cH','l'.encode('utf-8') if node.leaf else 'n'.encode('utf-8'),node.key_quantity) + b''.join([elem.term for elem in node.keys])
    
        for key,pointer in zip_longest(node.keys,node.pointers):
            if not node.leaf:
                if pointer:
                    encoded_record+=struct.pack(f'<IH',pointer[0],pointer[1])
                if key:
                    encoded_record+=struct.pack(f'<IH',key.data_pointer[0],key.data_pointer[1])
            else:
                encoded_record+=struct.pack('<IH',key.data_pointer[0],key.data_pointer[1])
        return encoded_record
        
        # if not node.leaf:
        #     temp_holder = []
        #     for pair in zip_longest(node.pointers,node.keys):
                
        
            
    
            
        
        
            

@dataclass
class TreeHolder(FixedBtree):
    TREEES:dict[int,FixedBtree] = field(default_factory=dict)

    
    def insertKey(self,treeKey:Key):
        

            
        if len(treeKey.term) not in self.TREEES.keys():
            degree = DEGREEPERLEN[Range.AVERAGE if 3<=len(treeKey.term) <=10 else Range.LONGSHORT]
            self.TREEES[len(treeKey.term)] = FixedBtree()
            self.TREEES[len(treeKey.term)].makeTree(degree,len(treeKey.term))
        
        self.TREEES[len(treeKey.term)].insertKey(treeKey)
       


    def storeTrees(self,dirpath):
        # quantity_workers = os.cpu_count() if len(self.TREEES)> os.cpu_count() else len(self.TREEES)
        # with multiprocessing.Pool(processes=quantity_workers) as mp:
        #     mp.starmap(
        #         self.storeTree,
        #         ((_,dirpath) for _ in self.TREEES.values())
        #     )
        for tree in self.TREEES.values():
            tree.storeTree(dirpath)
        
'''

    All terms sent to func have equal length.
'''




class TraverseThroughTree:
    
    def __init__(self,pathtofile : str,pathtoindex : str,terms:list[bytes]):
        '''
            pathtofile = pathtodir
        '''
        self.pathtofile = pathtofile
        self.sought_terms = terms
        self.keylen = len(self.sought_terms[0])
        self.indexpath = pathtoindex
        
        self.cachedTree = CachedTree()
        
        
        
    def traverse(self) -> list[Optional[bytes]]:
        
        
        
        try:
            with open(os.path.join(self.pathtofile,f'{self.keylen}btree.bin'),'rb') as self.treefile:
                self.treefile.seek(-struct.calcsize('<IH'),os.SEEK_END)
                # root_offset, root_length = struct.unpack('<IH',self.treefile.read())
                # os.lseek(self.treefile,os.SEEK_SET,root_offset)
                # root = self.treefile.read(root_length)
                # if struct.unpack_from('<c',root,0) == 'l'.encode('utf-8'):
                #     return self.__BSleaf(root)
                # else:
                #     return self.__BSnode(root)
                '''
                    Will contain either None or offset\length pair in each element respectively.
                '''
                
                record_properties = []
                
                for term in self.sought_terms:
                    cached_info = self.cachedTree.searchTerm(term)
                    if cached_info is not None:
                        if cached_info[0] == 'd':
                            record_properties.append(cached_info[1])
                            continue
                        offset,length = cached_info[1]
                    else:
                        offset,length = struct.unpack('<IH',self.treefile.read())
                    
                    '''
                        TODO: change returning value in that func to pair offset\length in index file instead of merely returning posting from index file,
                        for some speed up, because all postings correspond to specific not None result of term treating will be read at same time.
                        did it i guess
                    '''
                    
                    record_properties.append(self.__proxyLayer(term,offset,length))
        except OSError:
            raise RuntimeError
        return  self.__returnDataFromIndexFile(record_properties)
            
            
            
    def _BSnode(self,sought_term : bytes , keys : list[bytes], data_pointers :list[tuple[int,int]],\
        uppr_border:int = 0,lowr_border:int = 0,node_pointers : list[tuple[int,int]] = None) -> Optional[bytes]:
        # if keys is None:
        #     key_quantity = struct.unpack_from('<H',node,struct.calcsize('<c'))
        #     keys = [struct.unpack_from(f'<{self.keylen}s',node,struct.calcsize('cH') + i*struct.calcsize(f'{self.keylen}s')) for i in range(key_quantity)]
        #     pointers = [struct.unpack_from('<IH',node,struct.calcsize(f'<cH{key_quantity*self.keylen}') + i*6) for i in range(2*key_quantity+1)]
        #     uppr_border = key_quantity-1
        #     lowr_border = 0
        # if uppr_border-lowr_border==1:
        #     if keys[lowr_border]<self.sought_term < keys[uppr_border]:
        #         return self.__proxyLayer(node_pointers[uppr_border])
        # if uppr_border == lowr_border:
        #     if keys[uppr_border] != self.sought_term:
        #         return None
        #     return self.__returnDataFromIndexFile(data_pointers[uppr_border])
        if uppr_border==lowr_border:
            if sought_term == keys[uppr_border]:
                return data_pointers[uppr_border]
            child_node_offset,child_node_length = node_pointers[uppr_border if keys[uppr_border] > sought_term else uppr_border+1]
            # os.lseek(self.treefile.fileno(),child_node_offset,os.SEEK_SET)
            return self.__proxyLayer(sought_term,child_node_offset,child_node_length)
                                                      
        median = (uppr_border+lowr_border)//2
        # if keys[median]==self.sought_term:
        #     return self.__returnDataFromIndexFile(node_pointers[2*median+1])
        if keys[median] < sought_term:
            return self._BSnode(sought_term,keys,data_pointers,uppr_border,median+1,node_pointers)
        return self._BSnode(sought_term,keys,data_pointers,lowr_border,median,node_pointers)
         
    
    def _BSleaf(self,sought_term : bytes,keys : list[bytes],data_pointers : list[tuple[int,int]] ,\
        upper_bound:int=0,lower_bound:int=0,node_pointers : list[tuple[int,int]]=None) -> Optional[tuple[int,int]]:
        while(upper_bound!=lower_bound):
            median = (lower_bound+upper_bound)//2
            if keys[median] < sought_term:
                lower_bound = median+1
            else:
                upper_bound = median
        #return self.__returnDataFromIndexFile(data_pointers[upper_bound]) if keys[upper_bound] == sought_term else None
        return data_pointers[upper_bound] if keys[upper_bound] == sought_term else None 
        # if upper_bound == lower_bound:
        #     if keys[upper_bound]!= self.sought_term:
        #         return None
        #     return self.__returnDataFromIndexFile(data_pointers[upper_bound])
        # median = (upper_bound+lower_bound)//2
        # if self.sought_term > keys[median]:
        #     return self._BSleaf(keys,data_pointers,upper_bound,median+1)
        # return self._BSleaf(keys,data_pointers,median-1,lower_bound)
    
        

            
                
        
            
    '''
        have been catching situation ,which have no explanation to exist, hence, lseek discarded/ ( for some reasons in cases to simply move 
        cursor to beginning retreived negative value of offset after applying that func)
        perhaps mybad
    ''' 
    
    def __proxyLayer(self,sought_term,node_offset : int,node_length : int):
        # node_offset,node_length = struct.unpack_from('<IH',pointer)
        # os.lseek(self.treefile.fileno(),node_offset,os.SEEK_SET)
        
        
        self.treefile.seek(node_offset,os.SEEK_SET)
        # postion = self.treefile.tell()
        node = self.treefile.read(node_length)
        # position = self.treefile.tell()
        node_type,key_quantity, *_= struct.unpack_from('<cH',node,0)
        #key_quantity,*_ = struct.unpack_from('<H',node,struct.calcsize('<c'))
        
        
        terms = [struct.unpack_from(f'<{self.keylen}s',node,struct.calcsize('<cH') + i*struct.calcsize(f'{self.keylen}s'))[0] for i in range(key_quantity)]
        # b = node
        # pointers = [struct.unpack_from('<IH',node,struct.calcsize(f'<cH{key_quantity*self.keylen}') + i*6) for i in range(2*key_quantity+1)]
        # dp_pointers_tied = []
        
        temp_offset = struct.calcsize('<cH') +\
                struct.calcsize(f'<{key_quantity * self.keylen}s')
                
        node_pointers,data_pointers = [],[]
        if node_type == b'l':
            
            data_pointers = [struct.unpack_from('<IH',node,temp_offset+ 6*_) for _ in range(key_quantity)]
        else:
            for _ in range(2*key_quantity+1):
                cur_record = struct.unpack_from('<IH',node,temp_offset+ 6*_)
                if _%2==0:
                    node_pointers.append(cur_record)
                else:
                    data_pointers.append(cur_record)
        
        
        # for _ in range(key_quantity if node_type == b'l' else 2*key_quantity+1):
        #     dp_pointers_tied.append(struct.unpack_from('<IH',node,temp_offset+ 6*_))
        # dp_pointers_tied = [elem for elem in struct.unpack_from('<IH',node,struct.calcsize('<cH') + 6*_) for _ in range(key_quantity if\
        #     node_type == b'l' else 2*key_quantity+1)]
        
        
        
        
        # if node_type == b'l':
        #     data_pointers = [(elem[0],elem[1]) for elem in dp_pointers_tied]
        #     node_pointers = []
        
        
        # else:
        #     node_pointers = []
        #     data_pointers = []
        #     for id,record in enumerate(dp_pointers_tied):
        #         if id%2==0:
        #             node_pointers.append((record[0],record[1]))
        #         else:
        #             data_pointers.append((record[0],record[1]))
            
    
            
            
        # if type == 'l':
        #     return self.__BSleaf(keys,pointers,0,key_quantity-1)
        # return self.__BSnode()
        if len(self.sought_terms)>1:
            self.cachedTree.insertNode(key_quantity,terms,node_pointers,data_pointers)
        
        return self.__getattribute__(f'_BS{NODETYPE[node_type]}')(sought_term,terms,data_pointers,key_quantity-1,0,node_pointers) 
        
        
    def __returnDataFromIndexFile(self,record_properties : list[tuple[int,int] | None]) -> list[list[int] | None]:
        return_postings = []
        with open(self.indexpath, 'rb') as binary_index:
            for properties in record_properties:
                if properties is None:
                    return_postings.append(None)
                    continue
                    
                binary_index.seek(properties[0],os.SEEK_SET)
                return_postings.append(decodePackedBCD(binary_index.read(properties[1])))
                
        return return_postings
    




if __name__ == "__main__":
    # tree = TreeHolder()
    # for elem in DICTIONARY:
    #     tree.insertKey(Key(None,encodeString(elem)))
        
    # pass
    # a = Node(keys = [Key(1,b'fdadfaf'),Key(2,b'dfafdasf')],key_quantity=2)
    # b = weakref.proxy(a)
    # print(b)
    # del a
    # print(b)
    tree = FixedBtree()
    tree.makeTree(4,3)
    for elem in LENGTHDEPENDSDICTIONARY:
        tree.insertKey(Key([1,1],elem))
        
    tree.storeTree('./3tree.bin')
    res = TraverseThroughTree('./','./index.bin',[b'\x1c\x1d\x1a',b'\x22\x11\x33']).traverse()
    pass