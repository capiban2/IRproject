from mergepostrings import MergePostings
import os
import multiprocessing
import time
import sys
import asyncio
import re


def printInfo(val):
    print(os.getpid())

INDIR='/home/iv/Documents/mydir/kursovaya/temp'
STOPBORDER = 128

# class AsyncMergerSameFile:
#     def __init__(self,dirname:str,chunksize:int,offset:int,blocksize):
#         self.dirname = dirname
#         self.start_with_file_with_number = offset
#         self.quantity_files = chunksize
#         self.bk = blocksize
#     def __aiter__(self):
#         self.filedescriptors = [open(os.path.join(self.dirname,filename)) for filename in os.listdir(self.dirname)[self.start_with_file_with_number:self.start_with_file_with_number+self.quantity_files]]
#         self.buffer = [fd.read(self.bk).split(' ') for fd in self.filedescriptors]
#         self.queue = []
        
#         return self 
#     '''
#         files for merging have names like \d+.txt and result files will be have names like resultfile\d+.txt or smth like that.
#         (this information neccesary to properly select files for merging)
#         |---dir
#         |  |--0.txt
#         |  |--1.txt.
#         ---------
#         |  |---resfilesdir
#         |    |--
#         |    |--resultfile1.txt
#         |    |--resultfile2.txt
#         -------------------------
    
#     '''
    
#     async def __anext__(self):
#         is_closed = False
#         is_sorted = True
#         if sys.getsizeof(self.queue)<=STOPBORDER:
            
#             for id,subbufer in enumerate(self.buffer):
#                 if len(subbufer)<=2:
#                     is_closed = await self.__helper(id)
#                 try:
#                     if subbufer[0] not in self.queue:
#                         self.queue.append(subbufer[0])
#                         if is_sorted:
#                             is_sorted = False
#                     self.buffer[id].pop(0)
#                 except IndexError:
#                     raise RuntimeError
#         if not is_sorted:
#             self.queue.sort()
#         return_key = self.queue[0]
#         self.queue.pop(0)
#         if is_closed:
#             self.buffer = [self.buffer[i] for i in range(0,len(self.buffer)) if not self.filedescriptors[i].closed]
#             self.filedescriptors = [fd for fd in self.filedescriptors if not fd.close]
        
#         return return_key
            
        
                
                
#     async def __helper(self,id):
#         try:
#             flow = self.filedescriptors[id].read(self.bk).split(' ')
#         except IndexError:
#             return True
#         if flow ==['']:
#             self.filedescriptors[id].close()
#             return True
#         self.buffer[id][-1]+=flow[0]
#         self.buffer[id].extend(flow[1:])
#         return False

class MergerSameFile:
    def __init__(self,dirname,chunksize:int,offset:int,blocksize):
        self.dirname = dirname
        self.start_with_file_with_number = offset
        self.quantity_files = chunksize
        self.bk = blocksize
        self.filenames = [elem for elem in os.listdir(dirname)[self.start_with_file_with_number:self.start_with_file_with_number+self.quantity_files] if re.search(r'res\d+.out',elem) is None]
       
    def __iter__(self):
        self.filedescriptors = [open(os.path.join(self.dirname,filename)) for filename in os.listdir(self.dirname)[self.start_with_file_with_number:self.start_with_file_with_number+self.quantity_files]]
        self.buffer = [fd.read(self.bk).split(' ') for fd in self.filedescriptors]
        self.queue = []
        
        return self
    
    def __next__(self):
        is_closed = False
        is_sorted = True
        if len(self.queue)<=self.quantity_files:
            for id,subbufer in enumerate(self.buffer):
                if len(subbufer)==1:
                    ret_val = self.__helper(id)
                    if not is_closed and ret_val:
                        is_closed=True
                         
                
                if subbufer[0] not in self.queue:
                    self.queue.append(subbufer[0])
                    if is_sorted:
                        is_sorted = False
                self.buffer[id].pop(0)
            
                    
        if not is_sorted:
            self.queue.sort()
        try:   
            return_key = self.queue[0]
        except IndexError:
            raise StopIteration
        self.queue.pop(0)
        if is_closed:
            self.buffer = [buffer for buffer in self.buffer if len(buffer)>0]
            self.filedescriptors = [fd for fd in self.filedescriptors if not fd.closed]
        return return_key
                    
    def __helper(self,id):
        
        
             
        flow = self.filedescriptors[id].read(self.bk).split(' ')
                
        
        if flow == ['']:
            # print(f'File {self.filenames[id]} is removed.')
            self.filedescriptors[id].close()
            os.remove(os.path.join(self.dirname,self.filenames[id]))
            self.filenames.pop(id)
            
            return True
        self.buffer[id][0]+=flow[0]
        self.buffer[id].extend(flow[1:]) 
        return False
                
                
    
# class Merger:
#     def __init__(self,dirs):
#         self.dirnames = dirs
        
#     def mergeSameDir():

def mergeChunkFiles(dirname : str,offset_start_with : int,file_quantity  : int,blocksize : int,
                    resultfilename : str):
    with open(resultfilename,'w') as output_file:
        output = ''
        for term in MergerSameFile(dirname,file_quantity,offset_start_with,blocksize):
            if len(output)>512:
                output_file.write(output.strip(' '))
                output = ''
            output+= f'{term} '
        output_file.write(output.strip(' '))
    
        
    
            
def mergeSameDir(dirname:str, blocksize : int):
    quantity_files_per_proc = len(os.listdir(dirname))//os.cpu_count()
    with multiprocessing.Pool() as mp:
        mp.map(
            *(
                mergeChunkFiles(dirname, _*(quantity_files_per_proc+1),
                                quantity_files_per_proc,blocksize,os.path.join(dirname,f'{os.getpid()}{_}.out'))
                                for _ in range(len(os.listdir(dirname))//quantity_files_per_proc)
            )
        )
    mergeChunkFiles(dirname,0,os.cpu_count(),blocksize,os.path.join(dirname,'result.out'))
    
    # a = list((dirname, _*(quantity_files_per_proc+1),
    #                             quantity_files_per_proc,blocksize,os.path.join(dirname,f'{os.getpid()}{_}.out'))
    #                             for _ in range(len(os.listdir(dirname))//quantity_files_per_proc))
    # pass
    
        
        
        
    
if __name__ == '__main__':
    start = time.perf_counter()
    mergeChunkFiles('/home/iv/Documents/mydir/kursovaya/temp',0,300,512,'/home/iv/Documents/mydir/kursovaya/temp/res.out')
    print(f'{time.perf_counter() - start }')
    
    # start = time.perf_counter()
    # mergeSameDir('/home/iv/Documents/mydir/kursovaya/temp',512)
    # print(f'{time.perf_counter() - start}')
    # #def mergeChunkOfFiles(self,quantity:int,startwith:int):
        

# async def main(dirname,chunksize,offset,blocksize):
#     async for elem in AsyncMergerSameFile(dirname,chunksize,offset,blocksize):
#         #print(elem)
#         pass
        
        




# if __name__ == '__main__':
#     # start = time.perf_counter()
#     # asyncio.run(main(INDIR,3,0,512))
#     # print(time.perf_counter() - start)
#     start = time.perf_counter()
#     for elem in MergerSameFile(INDIR,3,0,512):
#         #print(elem)
#         pass
        
#     print(time.perf_counter() -start)

    
    # start = time.perf_counter()
    # with multiprocessing.Pool(processes=4) as mp:
    #    mp.map(
    #        printInfo, (val for val in range(os.cpu_count()))
    #    )
    # print(time.perf_counter()-start)
    
    
      
    