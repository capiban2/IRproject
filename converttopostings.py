import os
import struct
import re
import time
import sys
FILEPATH='/home/iv/Documents/mydir/kursovaya/testfilesstorage2'
POSTINGFILESPATH='home/iv/mydir/kursovaya/postingfilesstorage'
from encodeASCIIletters import encodeString
STOPSIZE=512*4*16
BLOCKSIZE=512

class IterThroughFiles:
    def __init__(self,path,listt,docIDs):
        self.listt =sorted(listt)
        self.docIDs=docIDs
        self.path = path
    def __iter__(self):
        self.docIDiter = iter(self.docIDs)
        self.curDocID=next(self.docIDiter)
        self.listiter = iter(self.listt)        
        with open(os.path.join(self.path,next(self.listiter)),'rb') as f:
            self.mainiter = iter(re.split(b' ',f.read()))
        
        return self
    
    # def __next__(self):
    #     if self.cur_flow ==[] or self.quantity_handled>len(self.cur_flow):
    #         if self.cur_flow!=[]:
    #             try:
    #                 self.cur_docID=next(self.docIDiter)
    #             except Exception:
    #                 raise StopIteration
    #         with open(os.path.join(self.path,next(self.listiter)),'rb') as f:
    #             self.cur_flow = re.split(b' ',f.read())
    #         self.quantity_handled = 0
    #     self.quantity_handled +=1
    #     return (self.cur_docID,self.cur_flow[self.quantity_handled-1].decode('utf8'))
    def __next__(self):
        try:
            cur_val = next(self.mainiter)
            return (self.curDocID,cur_val.decode('utf8'))
        except Exception:
            try:
                with open(os.path.join(self.path,next(self.listiter)),'rb') as f:
                    self.mainiter = iter(re.split(b' ',f.read()))
                self.curDocID = next(self.docIDiter)
                return (self.curDocID,next(self.mainiter).decode('utf8'))
            except StopIteration:
                raise StopIteration
                     
    
             

def making_posting(lisst,docID,pathtodirectory):
        
    posting = {}
    filenames = []
    for id,val in IterThroughFiles('/home/iv/Documents/mydir/kursovaya/testfilesstorage2',lisst,docID):
        
        if val not in posting.keys():
            posting[val] = set()
            posting[val].add(id)
            
        else:
            #Works fine only for Boolean retrieval, because isnt keep track of frequency and etc...
           posting[val].add(id) 
            
        if sys.getsizeof(posting)>=STOPSIZE:
            filenames.append(f'{len(filenames)}.bin')
            tmp = sorted(posting.keys())
            if '' in tmp:
                tmp.pop(0)
            with open(os.path.join(pathtodirectory,filenames[-1]),'wb') as file:
                for elem in tmp:
                    #file.write(f'{elem} : {posting[elem]}')
                   
                    #encoded = struct.pack(f'<2B{len(elem)}s{len(posting[elem])}Bs',len(elem),len(posting[elem]),elem.encode('utf-8'),*posting[elem],b' ')
                    file.write(struct.pack(f'<2B{len(elem)}s{len(posting[elem])}Bs',len(elem),len(posting[elem]),elem.encode('utf-8'),*posting[elem],b' '))
                posting = {}   
    filenames.append(f'{len(filenames)}.bin')
    tmp = sorted(posting.keys())
    if '' in tmp:
        tmp.pop(0)
    with open(os.path.join(pathtodirectory,filenames[-1]),'wb') as file:
        for elem in tmp:
         
            #file.write(f'{elem} : {posting[elem]}')
            file.write(struct.pack(f'<2B{len(elem)}s{len(posting[elem])}hs',len(elem),len(posting[elem]),elem.encode('utf-8'),*posting[elem],b' '))
        posting = {}
        
    return filenames



if __name__ == '__main__':
    start = time.perf_counter()
    res = making_posting(os.listdir(FILEPATH),[0,1,2],'/home/iv/Documents/mydir/kursovaya/postingfilesstorage')
    print(f'Time require to make posting : {time.perf_counter() - start}')
    print(res)
    pass
     
        
            
                


   