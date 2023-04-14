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
    this enums is used to distinguish between words of rare length and average;
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
    Range.LONGSHORT : 5,
    Range.AVERAGE : 10
}


NODETYPE : dict[str,str] = {
    'l' : 'leaf',
    'n' : 'node'
}

@dataclass(slots=True)
class Key:
    data_pointer : list[int] = field(default_factory=list)
    term : bytes =field(default=b'') 
    
@dataclass(slots=True)
class Node:
    leaf:bool = True
    key_quantity:int = field(default=0)
    keys:list[Key] = field(default_factory=list)
    pointers: list[ ReferencableNode | list[int] | None ] =field(default_factory=list)
    parrent_pointer : weakref.ProxyType= field(default = None)
    left_pointer : weakref.ProxyType= field(default = None)
    right_pointer : weakref.ProxyType= field(default = None)

class ReferencableNode(Node):
    __slots__ = ('__weakref__',)



 
    def __del__(self):
       print('delete')
    
class FixedBtree:
    def __init__(self):
        pass


    #on this step received keys looks like data_pointer and key encoded by custom encoding already. (not just utf-8 encoding)
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
            while number>0:
                if node_where_is_insert.keys[number].term < received_key.term:
                    break
                number-=1
            
                
            node_where_is_insert.keys.insert(number,received_key)
            return
        
        node_number = node_where_is_insert.key_quantity-1
        while node_number>0:
            if node_where_is_insert.keys[node_number].term < received_key.term:
                break
            node_number-=1
        if node_where_is_insert.pointers[node_number].key_quantity == self.degree-1:
            self.__split_child(node_where_is_insert,node_number)
            if node_where_is_insert.keys[node_number].term < received_key.term:
                node_number+=1
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
            fdas
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
    ''' 
    
    def storeTree(self,filepath : str = '/tmp/filepath.bin'):
        with open(filepath,'wb') as binary_output:
            file_offset = 0
            current_node = self.root
            if self.tree_length > 1:
                while not current_node.pointers[0].leaf:
                    current_node = current_node.pointers[0]
                while current_node is not None:
                    file_offset+=self.__storeOneLevel(file_offset,current_node,'right',binary_output)
                    current_node = current_node.parrent_pointer
            
            encoded_root = self.__packNode(self.root) 
            # if len(encoded_root)!=((self.keylen+12)*self.root.key_quantity+9):
            #     raise RuntimeError(f'Encoded size isnt fit in expected size : ({len(encoded_root)} : {((self.keylen+13)*self.root.key_quantity+8)})') 
            binary_output.write(encoded_root + struct.pack('<IH',file_offset,len(encoded_root)))
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
        Each code of data_pointers encoding contain to digits, offset in sought file and length of sought record respectively,
        both of which are encoded as unsigned integer.
        
        3. Node is inner
        Almost all actions listed above will be the same, except in encoding of data_pointers will be appended one pointer in this file to child node,
        associated with specific key of this node.

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
        
            
    
            
        
        
            

@dataclass
class TreeHolder:
    TREEES:dict[int,FixedBtree] = field(default_factory=dict)

    
    def insertKey(self,treeKey:Key):
        

            
        if len(treeKey.term) not in self.TREEES.keys():
            degree = DEGREEPERLEN[Range.AVERAGE if 4<=len(treeKey.term) <=9 else Range.LONGSHORT]
            self.TREEES[len(treeKey.term)] = FixedBtree()
            self.TREEES[len(treeKey.term)].makeTree(degree,len(treeKey.term))
        
        self.TREEES[len(treeKey.term)].insertKey(treeKey)
       

class TraverseThroughTree:
    
    def __init__(self,pathtofile : str,term):
        self.pathtofile = pathtofile
        self.sought_term = term
        self.keylen = int(re.search('\d+',os.path.basename(pathtofile)).group(0))
        
    def traverse(self) -> Optional[bytes]:
        try:
            with open(self.pathtofile) as self.treefile:
                os.lseek(tree,os.SEEK_END,-struct.calcsize('<IH'))
                # root_offset, root_length = struct.unpack('<IH',self.treefile.read())
                # os.lseek(self.treefile,os.SEEK_SET,root_offset)
                # root = self.treefile.read(root_length)
                # if struct.unpack_from('<c',root,0) == 'l'.encode('utf-8'):
                #     return self.__BSleaf(root)
                # else:
                #     return self.__BSnode(root)
                return self.__proxyLayer(self.treefile.read())
        except OSError:
            raise RuntimeError
            
            
            
    def __BSnode(self,keys : list[bytes] = None, pointers : list[bytes] = None,uppr_border:int = 0,lowr_border:int = 0) -> Optional[bytes]:
        # if keys is None:
        #     key_quantity = struct.unpack_from('<H',node,struct.calcsize('<c'))
        #     keys = [struct.unpack_from(f'<{self.keylen}s',node,struct.calcsize('cH') + i*struct.calcsize(f'{self.keylen}s')) for i in range(key_quantity)]
        #     pointers = [struct.unpack_from('<IH',node,struct.calcsize(f'<cH{key_quantity*self.keylen}') + i*6) for i in range(2*key_quantity+1)]
        #     uppr_border = key_quantity-1
        #     lowr_border = 0
        if uppr_border-lowr_border==1:
            if keys[lowr_border]<self.sought_term < keys[uppr_border]:
                return self.__proxyLayer(pointers[2*lowr_border])
        if uppr_border == lowr_border:
            if keys[uppr_border] != self.sought_term:
                return None
            return self.__returnDataFromIndexFile(pointers[2*uppr_border+1])
        
        median = (uppr_border+lowr_border)//2
        if keys[median]==self.sought_term:
            return self.__returnDataFromIndexFile(pointers[2*median+1])
        if keys[median] < self.sought_term:
            return self.__BSnode(keys,pointers,uppr_border,median+1)
        return self.__BSnode(keys,pointers,lowr_border,median-1)
         
    
    def __BSleaf(self,keys : list[bytes],pointers : list[bytes], upper_bound:int,lower_bound:int) -> Optional[bytes]:
        if upper_bound == lower_bound:
            if keys[upper_bound]!= self.sought_term:
                return None
            return self.__returnDataFromIndexFile(pointers[upper_bound])
        median = (upper_bound+lower_bound)//2

            
                
        
            
    
    def __proxyLayer(self,pointer : bytes):
        node_offset,node_length = struct.unpack('<IH',pointer)
        os.lseek(self.treefile,os.SEEK_SET,node_offset)
        node = self.treefile.read(node_length)
        type,key_quantity, *_= struct.unpack_from('<cH',node,0)
        #key_quantity,*_ = struct.unpack_from('<H',node,struct.calcsize('<c'))
        keys = [struct.unpack_from(f'<{self.keylen}s',node,struct.calcsize('cH') + i*struct.calcsize(f'{self.keylen}s')) for i in range(key_quantity)]
        pointers = [struct.unpack_from('<IH',node,struct.calcsize(f'<cH{key_quantity*self.keylen}') + i*6) for i in range(2*key_quantity+1)]
        # if type == 'l':
        #     return self.__BSleaf(keys,pointers,0,key_quantity-1)
        # return self.__BSnode()
        return self.__getattribute__(f'__BS{NODETYPE[type]}')(keys,pointers,0,key_quantity-1) 
        
        
    def __returnDataFromIndexFile(self,test):
        pass
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
        
    tree.storeTree('./tree.bin')
    pass