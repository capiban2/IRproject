from __future__ import annotations
import time
import struct
import sys
class Node:
    __slots__=['value','left','right']
    def __init__(self,value,left=None,right=None):
        self.value = value
        self.left = left
        self.right = right

class BinaryTree:
    def __init__(self,value):
        self.root = Node(value)
        
    @property
    def left(self):
        return self.root.left
    
    @left.setter 
    def left(self,value):
        self.root.left = value
    
    @property
    def right(self):
        return self.root.right
    
    @right.setter
    def right(self,value):
        self.root.right = value

        
        
MAPPING={
    '00':' ',
    '0100' : 'a',
    '010100': 'b',
    '010101' : 'c',
    '01011' : 'd',
    '0110' : 'e',
    '011100' : 'f',
    '011101' : 'g',
    '01111' : 'h',
    '1000' : 'i',
    '1001000' : 'j',
    '1001001' : 'k',
    '100101' : 'l',
    '10011' : 'm',
    '1010' : 'n',
    '1011' : 'o',
    '110000' : 'p',
    '110001' : 'q',
    '11001' : 'r',
    '1101' : 's',
    '1110' : 't',
    '111100' : 'u',
    '111101' : 'v',
    '111110' : 'w',
    '11111100' : 'x',
    '11111101' : 'y',
    '1111111' : 'z'
}

MAPPING_REVERSE={
    ' ':'00',
     'a': '0100',
 'b':     '010100',
  'c':     '010101',
  'd':     '01011',
      'e': '0110',
      'f': '011100',
      'g': '011101',
      'h': '01111',
      'i': '1000',
      'j': '1001000',
     'k': '1001001',
      'l': '100101',
      'm': '10011',
      'n': '1010',
      'o': '1011',
      'p': '110000',
      'q': '110001',
      'r': '11001',
      's': '1101',
      't': '1110',
      'u': '111100',
      'v': '111101',
      'w': '111110',
      'x': '11111100',
     'y': '11111101',
     'z': '1111111',
     '\n' : '01'
}
        
        
def constructor():
    tree =BinaryTree('-1')
    
    #First layer
    tree.right = Node('1')
    tree.left = Node('0')
    left1 = tree.left
    right1 = tree.right
    
    #2nd layer
    left1.right = Node('1')
    left1.left = Node('0')#passed by ,means whitespace
    right1.right = Node('1')
    right1.left = Node('0')
    
    #3rd layer
    left1.right.left = Node('0')
    left1.right.right = Node('1')
    right1.right.right = Node('1')
    right1.right.left = Node('0')
    right1.left.right = Node('1')
    right1.left.left = Node('0')
    
    #4th layer
    left1.right.left.right = Node('1')
    left1.right.left.left = Node('0')#passed by;means 'A'
    left1.right.right.left = Node('0')#passed by;means 'E'
    left1.right.right.right = Node('1')
    right1.right.right.right = Node('1')
    right1.right.right.left = Node('0')#passed by;means 'T'
    right1.right.left.left = Node('0')
    right1.right.left.right = Node('1')#passed by;means 'S
    right1.left.right.right = Node('1')#passed by;means 'O'
    right1.left.right.left = Node('0')#passed by;menas 'N
    right1.left.left.left = Node('0') #passed by;means 'I'
    right1.left.left.right = Node('1')
    
    #5th layer
    left1.right.left.right.left =Node('0')
    left1.right.left.right.right = Node('1') #passed by;means 'D'
    left1.right.right.right.right = Node('1')#passed by;means 'H'
    left1.right.right.right.left = Node('0')
    right1.right.right.right.right = Node('1')
    right1.right.right.right.left = Node('0')
    right1.right.left.left.right = Node('1')#passed by;means 'R'
    right1.right.left.left.left = Node('0')
    right1.left.left.right.right = Node('1')#passed by;means 'M'
    right1.left.left.right.left = Node('0')
    
    #6th layer
    left1.right.left.right.left.left = Node('0')#passed by;means 'B'
    left1.right.left.right.left.right = Node('1')#passed by;means 'C'
    left1.right.right.right.left.left = Node('0')#passed by;means 'F'
    left1.right.right.right.left.right = Node('1')#passed by;means 'G'
    right1.right.right.right.left.right = Node('1') #passed by;means 'V'
    right1.right.right.right.left.left = Node('0') #passed by;means 'U'
    right1.right.right.right.right.left = Node('0')#passed by;means 'W'
    right1.right.right.right.right.right = Node('1')
    right1.right.left.left.left.right = Node('1')#passed by;means 'Q'
    right1.right.left.left.left.left = Node('0')#passed by;means 'P'
    right1.left.left.right.left.left = Node('0')
    right1.left.left.right.left.right = Node('1')#passed by; means 'L'
    
    #7th layer
    right1.right.right.right.right.right.left = Node('0')
    right1.right.right.right.right.right.right = Node('1')#passed by; means 'Z'
    right1.left.left.right.left.left.left = Node('0')#passed by; means 'J'
    right1.left.left.right.left.left.right = Node('1')#passed by;means 'K'
    
    #8th layer
    right1.right.right.right.right.right.left.right = Node('1')#passed by;means 'Y'
    right1.right.right.right.right.right.left.left = Node('0')#passed by;means 'X'
    
    
def decode(tree:BinaryTree,encoded_string)->str:
    result_string = ''
    current_code = ''
    iterator_through = tree
    for letter in encoded_string:
        if iterator_through.left == None and iterator_through.right == None:
            result_string+=MAPPING[current_code+letter]
            iterator_through =tree
            continue
        iterator_through = iterator_through.left if letter =='0' else iterator_through.right
        current_code+=letter
        
    return result_string
            

def encodeString(string:str):
    encoded_string =  (fromBinToDec(elem) for elem in VBEncoding(''.join([MAPPING_REVERSE[elem] for elem in string])))
    #print(encoded_string)
    return b''.join([struct.pack('<B',elem) for elem in encoded_string])
    #return encoded_string

   
def fromBinToDec(string)->int:
    res = 0
    for let in string:
        res = res<<1

        if let=='1':
            res+=1
        
    return res
def VBEncoding(prepared_string:str)->hex:
    string_len = len(prepared_string)
    elements = ['0' + prepared_string[i:i+7] for i in range(0,string_len-string_len%7,7)]
    last_elem = '1'*((string_len)%7+1)+'0'+prepared_string[string_len-string_len%7:]+'0'*(16-2*((string_len)%7+1))
    elements.extend([last_elem[0:8],last_elem[8:]])
    
    #yield elements
    #yield last_elem[0:8]
    #yield last_elem[8:]
    
    #print(elements)
    return elements
    # print(last_elem)
    # print(len(last_elem))
    #res = [struct.pack('<',elem) for elem in elements]
    #print(res)
        
    

if __name__ == '__main__':
    # start = time.perf_counter()
    # constructor()
    # print(f'Time consumed by tree constructor : {time.perf_counter() - start}')
    
    #string = 'abcetyfdafsgfdgsfdfdaf'
    string = 'stno'
    #start = time.perf_counter()
    encode = encodeString(string)
    print(encode)
    print(len(encode))
    #print(f'Time consumed by custom encoding : {time.perf_counter() - start}')
    #start = time.perf_counter()
    encode = string.encode('utf-8')
    print(encode)
   # print(f'Time consumed by build-in encoding : {(time.perf_counter() - start)}')
    
    # start = time.perf_counter()
    # res = fromBinToDec('10010000')
    # print(f'Time neccessary for 1 convertion : {(time.perf_counter() - start)}')
    # print(res)
    # start = time.perf_counter()
    # res = fromBinToDec2('10010000')
    # print(f'Time neccessary for 2 convertion : {(time.perf_counter() - start)}')
    # print(res)
    #string2 = b'abcetyfdafsgfdgsfdfdaf'
    #print(encode)
    #print(len(encode))
    #print(len(string2))
    #VBEncoding(encode) 
    #print(fromBinToDec('10010000'))
    # print(encode)
    # print(f'Time consumed by encoding with tree: {time.perf_counter() - start}')
    # print(len(encode))
    # start = time.perf_counter()
    # encode2 = string.encode('utf-8')
    # print(f'Time consumed by encoding with tree: {time.perf_counter() - start}')
    # print(len(encode2)) 
    # pass
    print(116%7)