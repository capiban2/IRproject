import sys
import os
import struct
from operator import itemgetter
import time
BLOCKSIZE=1024
SIZEBORDER=128

class MergePostings:
    def __init__(self,intermediate_posting_storage:str,result_posting_filename):
        self.interdir = intermediate_posting_storage
        self.res_filename = result_posting_filename
        
    def __iter__(self):
        self.filedescriptors = [open(os.path.join(self.interdir,file),'rb') for file in os.listdir(self.interdir)]
        self.buffer = [self.filedescriptors[i].read(BLOCKSIZE).split(b' ') for i in range(len(self.filedescriptors))]
        self.queue = {}
        # for elem in self.buffer:
        #     string_len,posting_len = struct.unpack_from('<BB',elem[0],0)
        #     current_word = struct.unpack_from(f'{string_len}s',elem[0],offset = struct.calcsize('<BB'))
        #     if current_word in self.queue.keys():
        #         self.queue[current_word].extend(list(struct.unpack_from(f'<{posting_len}h',elem,offset = struct.calcsize(f'<BB{string_len}s'))))
        #     else:
        #         self.queue[current_word] = list(struct.unpack_from(f'<{posting_len}h',elem,offset = struct.calcsize(f'<BB{string_len}s')))
        #     elem.pop(0)
        return self 
    
    def __next__(self):
        is_closed = False
        if len(self.queue.keys())<=SIZEBORDER: 
            for id,subbufer in enumerate(self.buffer):
                if len(subbufer) <= 1:
                    flow = self.filedescriptors[id].read(BLOCKSIZE) 
                    if len(flow) == 0:
                        self.filedescriptors[id].close()
                        is_closed = True
                        continue
                    flow = flow.split(b' ')
                    self.buffer[id][0]+=flow[0]
                    self.buffer[id].extend(flow[1:])
                   # subbufer = self.buffer[id]
                    flow = ''
                string_len,posting_len = struct.unpack_from('<BB',subbufer[0],0)
                current_key,*posting = struct.unpack_from(f'<{string_len}s{posting_len}B',subbufer[0],offset = struct.calcsize('<BB'))
                current_key = current_key.decode('utf-8') 
                if current_key in self.queue.keys():
                    self.queue[current_key].update(posting)
                else:
                    self.queue[current_key] = set(posting)
                self.buffer[id].pop(0) 
        self.queue = dict(sorted(self.queue.items(),key = itemgetter(0)))
        res_key = next(iter(self.queue.items()))
        self.queue.pop(res_key[0])
        if is_closed:
            self.buffer = [self.buffer[i] for i in range(len(self.buffer)) if not self.filedescriptors[i].closed]
            self.filedescriptors = [fd for fd in self.filedescriptors if not fd.closed]
            

        return res_key     

                
'''
 c = struct.unpack_from(f'<{b[0]}s{b[1]}h',a,offset = struct.calcsize('<BB'))
 b = struct.unpack_from('<BB',a,0)
'''

if __name__ == '__main__':
    test = MergePostings('/home/iv/Documents/mydir/kursovaya/postingfilesstorage','home/iv/Documents/mydir/kursovaya/resultpostingstorage')
    start = time.perf_counter()
    for item in test:
        print(item)
       #pass
   
    print(f'Time consumed by merging :{time.perf_counter()-start}')