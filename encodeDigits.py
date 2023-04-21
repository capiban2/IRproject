import struct

def DensePackDecimal(x,y,z):
    typ = (x&8)>>1 | (y&8)>>2 | (z&8)>>1
    match typ:
        case 0:
            dpd = (x<<7)|(y<<4)|z
        case 1:
            dpd = (x<<7)|(y<<4)|0x8
        case 2:
            dpd = (x<<7)|(z&6)<<4|(y&1)<<4|0xa
        case 3:
            dpd = (x<<7) | (y&1)<<4 | 0x4e
        case 4:
            dpd = (z&6)<<7 | (x&1)<<7 | (y)<<4 | 0xc
        case 5:
            dpd = (y&6)<<7 | (x&1)<<7 | ((y&1)<<4 | 0x2e)
            
        case 6:
            dpd = (z&6)<<7 | (x&1) <<7 | ((y&1)<<4 | 0xe)
         
        case 7:
            dpd = (x&1)<<7 | (y&1)<<4 | 0x6e
    dpd|=(z&1)  
    return dpd


'''
    little-endian like
'''


class IterateOverNibbles:
    def __init__(self,bytestring:bytes):
        self.bytestring = bytestring
        
        
    def __iter__(self):
        self.nibbles = []
        for byte in self.bytestring:
            self.nibbles.append(byte&0xf)
            self.nibbles.append((byte&0xf0)>>4)
        self.nibbleiter = iter(self.nibbles)
        return self
    
    def __next__(self):
        try:
            return next(self.nibbleiter)
        except StopIteration:
            raise StopIteration
        
            #return byte&0x0f
            
        

def packedBCD(integers : list[int]) ->bytes:
    encoded = b''
    state = False
    byte = 0
    for digit in [integers[0]] + [integers[id]-integers[id-1] for id in range(1,len(integers))]:
        
        while(digit>0):
            
            remainder = digit%10
            if state:
                
                encoded+=struct.pack('B',byte|(remainder<<4))
                state = False
                byte = 0
            else:
                byte = remainder
                state = True
           
            digit//=10
        if state:
            encoded+=struct.pack('B',byte|(0xf<<4))
            state = False
            byte = 0
        else:
            byte = 0xf
            state = True
    
       
    return encoded
    
                
            
def decodePackedBCD(bytestring:bytes)->list[int]:
    result_gaps = []
    current_gap = 0
    current_degree = 0
    for nibble in IterateOverNibbles(bytestring):
        if nibble == 0xf:
            if not result_gaps:
                result_gaps.append(current_gap)
            else:
                result_gaps.append(result_gaps[-1]+current_gap)
            current_gap = current_degree = 0
            continue
        current_gap+= (nibble)*(10**current_degree)
        current_degree+=1
    if current_gap:
        result_gaps.append(result_gaps[-1]+current_gap)
    return result_gaps
        
# if __name__ == '__main__':
#     # a = b'\xfa\xaa\xfa'
#     # for elem in IterateOverNibbles(a):
#     #     print(elem)
    # a = [123,13,234,33,500,60]
    # ecnoded = packedBCD(a)
    # decoded = decodePackedBCD(ecnoded)
    # pass
#     a = [2,13,24,33,63,101]
#     a1 = [22,43,58,99,123,231,400]
#     a2 = [1,2,3,4,5,6,7,12,56]
#     #a = [2,6]
#     encoded = packedBCD(a)
#     encoded1 = packedBCD(a1)
#     assert encoded1 == b'\x22\x1f\xf2\x15\x1f\xf4\x24\x8f\x10\x9f\x16'
#     encoded2 = packedBCD(a2)
#     assert encoded2 == b'\xf1\xf1\xf1\xf1\xf1\xf1\xf1\xf5\x44'
#     decoded = decodePackedBCD(encoded)
#     decoded1  = decodePackedBCD(encoded1)
#     decoded2 = decodePackedBCD(encoded2) 
#     assert decoded == a
#     assert decoded1 == a1
#     assert decoded2 == a2
#     print(decoded)
#     pass
    