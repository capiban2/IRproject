from mergepostrings import MergePostings
import os
import multiprocessing
import time
import sys
import asyncio
import re
import aiofiles.os
import itertools
import shutil
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
    def __init__(self,dirname,filenames,chunksize,blocksize):
        self.dirname = dirname
        self.filenames = filenames
        self.quantity_files = chunksize
        self.bk = blocksize
        
       
    def __iter__(self):
        self.filedescriptors = [open(os.path.join(self.dirname,filename)) for filename in self.filenames]
        self.buffer = [fd.read(self.bk).split(' ') for fd in self.filedescriptors]
        self.previous = ''
        self.queue = []
        
        return self
    '''
        TODO : must add main Main preporty to actually MERGE files// 
    '''
    def __next__(self):
        is_closed = False
        is_sorted = True
        if len(self.queue)<self.quantity_files:
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
                #subbufer.pop(0)
                
            
                    
        if not is_sorted:
            self.queue.sort()
        try:   
            return_key = self.queue[0]
        except IndexError:
            raise StopIteration
        self.queue.pop(0)
        if is_closed:
            self.buffer = [buffer for _,buffer in enumerate(self.buffer) if len(buffer)>0 and self.filedescriptors[_].closed]
            self.filedescriptors = [fd for fd in self.filedescriptors if not fd.closed]
        return return_key
                    
    def __helper(self,id):
        
        
        try: 
            flow = self.filedescriptors[id].read(self.bk).split(' ')
        except IndexError:
            return True
                
        
        if flow == ['']:
            # print(f'File {self.filenames[id]} is removed.')
            self.filedescriptors[id].close()
            
            #self.filenames.pop(id)
            
            return True
        self.buffer[id][0]+=flow[0]
        self.buffer[id].extend(flow[1:]) 
        return False
                

class MergeDifferentFiles:
    def __init__(self,dirname : str,blocksize : int) ->None:
        self.dirname = dirname
        self.filenames = os.listdir(dirname)
        self.mapping = {filename:_+1 for _,filename in enumerate(self.filenames)}
        
        self.blksz = blocksize
        self.quantity_files = len(self.filenames)
        
    def __iter__(self):
        self.filedescriptors = [open(os.path.join(self.dirname,_)) for _ in self.filenames]
        self.buffer = [fd.read(self.blksz).split(' ') for fd in self.filedescriptors]
        self.queue : dict[str,list[int]]= {}
        self.queue_keys = []
        
        return self
    
    
    '''
        __next__ returns tuple, containing current term and his posting.
    '''
    
    def __next__(self)->tuple[str,list[int]]:
        is_closed = False
        is_sorted = True
        if len(self.queue)<=self.quantity_files:
            for id,subbufer in enumerate(self.buffer,start = 1):
                
                if len(subbufer)==1:
                    if not is_closed and self.__helper(id):
                        is_closed = True
                        
                current_term = subbufer[0]
                subbufer.pop(0)
                    
                    
                    
                    
                if current_term not in self.queue_keys:
                    self.queue[current_term] = [id]
                    self.queue_keys.append(current_term)
                    if is_sorted:
                        is_sorted = False
                else:
                    self.queue[current_term].append(id)
        if not is_sorted:
            sorted(self.queue_keys) 
        try:
            ret_term = self.queue_keys[0]
            return_value = (ret_term,self.queue[ret_term])
        except IndexError:
            raise StopIteration
        
        self.queue.pop(ret_term)
        self.queue_keys.pop(0)
        if is_closed:
            self.buffer = [buffer for _,buffer in enumerate(self.buffer) if len(buffer)>0 and self.filedescriptors[_].closed]
            self.filedescriptors = [fd for fd in self.filedescriptors if not fd.closed]
        return return_value
                        
                        
        
        
    def __helper(self,id):
        try: 
            flow = self.filedescriptors[id-1].read(self.blksz).split(' ')
        except IndexError:
            return True
                
        
        if flow == ['']:
            # print(f'File {self.filenames[id]} is removed.')
            self.filedescriptors[id-1].close()
            
            #self.filenames.pop(id)
            
            return True
        self.buffer[id-1][0]+=flow[0]
        self.buffer[id-1].extend(flow[1:]) 
        return False
        
    
# class Merger:
#     def __init__(self,dirs):
#         self.dirnames = dirs
        
#     def mergeSameDir():

def mergeChunkFiles(dirname : str,chunk_of_files : list[str] ,chunksize : int ,blocksize : int,
                    resultfilename : str):
    # print(f'Process {os.getpid()} was launched {time.perf_counter()}')
    # filenames = (elem for elem in os.listdir(dirname)[offset_start_with:offset_start_with+file_quantity] if re.search(f'result{epoch}\d+.out',elem) is None) \
    #     if epoch > 0 else os.listdir(dirname)[offset_start_with:offset_start_with+file_quantity]
    #pass
    # print(f'Received in merger func, {resultfilename}:')
    # print(chunk_of_files)
    if len(chunk_of_files)!=chunksize:
        raise RuntimeError
    with open(resultfilename,'w') as output_file:
        output = ''
        for term in MergerSameFile(dirname,chunk_of_files,chunksize,blocksize):
            if len(output)>512:
                output_file.write(output.strip(' '))
                output = ''
            output+= f'{term} '
        output_file.write(output.strip(' '))
    
    # for filename in filenames:
    #     print(f'Process {os.getpid()} had removed {os.path.join(dirname,filename)}')
    #     os.remove(os.path.join(dirname,filename))
    
    # for filename in chunk_of_files:
    #     try:
    #         os.remove(os.path.join(dirname,filename))
    #     except Exception:
    #         print(Exception)
    
    
    # for filename in chunk_of_files:
    #     try:
    #         fd = open(os.path.join(dirname,filename))
    #     except FileNotFoundError:
    #         continue
    #     else:
    #         print(f'File {filename}didnot deleted!')
        
    #asyncio.run(garbageCollector(dirname,offset_start_with,file_quantity))
   
# async def garbageCollector(dirname:str,offset_start_with : int,file_quantity :int):
#     await asyncio.gather(
#         *(aiofiles.os.remove(os.path.join(dirname,filename)) for filename in os.listdir(dirname)[offset_start_with:offset_start_with+file_quantity])
#     ) 
        
    
            
# def mergeSameDir(dirname:str, blocksize : int):
#     quantity_files_per_proc = len(os.listdir(dirname))//os.cpu_count()
#     with multiprocessing.Pool() as mp:
#         mp.map(
#             *(
#                 mergeChunkFiles(dirname, _*(quantity_files_per_proc+1),
#                                 quantity_files_per_proc,blocksize,os.path.join(dirname,f'result0{os.getpid()}.out'),0)
#                                 for _ in range(os.cpu_count())
#             )
#         )
#     with multiprocessing.Pool(processes=os.cpu_count()//2) as mp:
#         mp.map(
#             *(
#                 mergeChunkFiles(dirname,2*_,2,
#                                 blocksize,os.path.join(dirname,f'result1{os.getpid()}.out'),1)
#                 for _ in range(os.cpu_count()//2)
#             )
#         )
#     with multiprocessing.Pool(processes=2) as mp:
#         mp.map(
#             *(
#                 mergeChunkFiles(dirname,_*os.cpu_count()//2,os.cpu_count()//2,
#                                 blocksize,os.path.join(dirname,f'result2{os.getpid()}.out'),2)
#             )
#         )
        
#     mergeChunkFiles(dirname,0,2,blocksize,f'{os.path.basename(dirname)}.out',3)
    
#     #mergeChunkFiles(dirname,0,os.cpu_count(),blocksize,os.path.join(dirname,'result.out'))
    
#     # a = list((dirname, _*(quantity_files_per_proc+1),
#     #                             quantity_files_per_proc,blocksize,os.path.join(dirname,f'{os.getpid()}{_}.out'))
#     #                             for _ in range(len(os.listdir(dirname))//quantity_files_per_proc))
#     # pass
    
    
'''
    TODO: Move result file in other place, common storage(flatten nesting)
'''
def mergeSameDirs(dirnames : list[str],blksize : int,finalstoragepath : str) ->None:
    with multiprocessing.Pool() as mp:
        for dir in dirnames:
            if len(os.listdir(dir))<=os.cpu_count():
                continue
            dirlist = os.listdir(dir)
            quantity_files_per_proc = len(dirlist)//os.cpu_count()+1
            files_ranges = [dirlist[_*(quantity_files_per_proc):(1+_)*(quantity_files_per_proc)] for _ in range(os.cpu_count())]
            # print(files_ranges)
            iiterative_variable = [(dir,_,len(_),blksize,os.path.join(dir,f'result0{id}.out')) for id,_ in enumerate(files_ranges)]
            work = mp.starmap_async(
                mergeChunkFiles,iiterative_variable
                            
            )
            work.wait()
            for filename in dirlist:
                os.remove(os.path.join(dir,filename))
    for dir in dirnames:
        mergeChunkFiles(dir,os.listdir(dir),os.cpu_count(),blksize,f'{os.path.join(dir,os.path.basename(dir).split(".")[0])}.out')
        os.rename(f'{os.path.join(dir,os.path.basename(dir))}.out',f'{os.path.join(finalstoragepath,os.path.basename(dir))}.out')
        shutil.rmtree(dir)
        


def mergeSameDir1(dirname : str, blocksize : int,finalstoragepath : str) ->None:
    quantity_files_per_proc = len(os.listdir(dirname))//os.cpu_count()+1
    # data_to_prcoesses = [(_*(quantity_files_per_proc+1),quantity_files_per_proc,
    #               os.path.join(dirname,f'result0{_}.out'),0) for _ in range(os.cpu_count())]
    # with multiprocessing.Pool() as mp:
    #     mp.map(
            
    #             mergeChunkFiles,
    #             # ((dirname, _*(quantity_files_per_proc+1),
    #             #                 quantity_files_per_proc,blocksize,os.path.join(dirname,f'result0{os.getpid()}.out'),0) for _ in range(len(os.listdir(dirname)))
    #            (dirname for _ in data_to_prcoesses),
    #            (_[0] for _ in data_to_prcoesses),
    #            (blocksize for _ in data_to_prcoesses),
    #            (_[1] for _ in data_to_prcoesses),
    #            (_[2] for _ in data_to_prcoesses)
               
               
                
    #         )
    # a = list((dirname,_*(quantity_files_per_proc+1),
    #           quantity_files_per_proc,blocksize,os.path.join(dirname,f'result0{_}.out')) for _ in range(os.cpu_count()))
    # pass
    dirlist = os.listdir(dirname)
    # test = list((dirname,dirlist[_*(quantity_files_per_proc+1):(1+_)*(quantity_files_per_proc) +_],
    #           blocksize,os.path.join(dirname,f'result0{_}.out')) for _ in range(os.cpu_count()))
    # pass
    if len(dirlist)>os.cpu_count():
        
        with multiprocessing.Pool() as mp:
            mp.starmap(
                mergeChunkFiles,
                ((dirname,dirlist[_*(quantity_files_per_proc):(1+_)*(quantity_files_per_proc)],
                quantity_files_per_proc, blocksize,os.path.join(dirname,f'result0{_}.out')) for _ in range(os.cpu_count()))
            )
    # dirlist = os.listdir(dirname)
    # with multiprocessing.Pool() as mp:
    #     mp.starmap(
    #         mergeChunkFiles,
    #         ((dirname, dirlist[_*2 : (_+1)*2], 2 ,blocksize,os.path.join(dirname,f'result1{_}.out')) for _ in range(os.cpu_count()//2))
    #     )
    
     
    mergeChunkFiles(dirname,os.listdir(dirname),os.cpu_count(),blocksize,f'{os.path.join(dirname,os.path.basename(dirname).split(".")[0])}.out')
    
    os.rename(f'{os.path.join(dirname,os.path.basename(dirname))}.out',f'{os.path.join(finalstoragepath,os.path.basename(dirname))}.out') 
    os.rmdir(dirname)
    # a =  list((dirname,_*(quantity_files_per_proc+1),quantity_files_per_proc,blocksize,
    #               os.path.join(dirname,f'result0{os.getpid()}.out'),0) for _ in range(os.cpu_count()))
    # pass
    
        
        


 
# if __name__ == '__main__':
    # start = time.perf_counter()
    # mergeChunkFiles('/home/iv/Documents/mydir/kursovaya/temp',0,300,512,'/home/iv/Documents/mydir/kursovaya/temp/res.out')
    # print(f'{time.perf_counter() - start }')
    #print(os.path.basename(INDIR))
    
    
    # start  = time.perf_counter() 
    # mergeSameDir1('/home/iv/Documents/mydir/kursovaya/testfiles',512)
    # print(time.perf_counter()-start)
    
    # a = [(_*2,(_+1)*2) for _ in range(os.cpu_count()//2)]
    # print(a)
    # # quantity_files_per_proc = len(os.listdir(INDIR))//os.cpu_count()+1
    # dirlist = os.listdir(INDIR)
    
    # a = [dirlist[_*(quantity_files_per_proc):(1+_)*(quantity_files_per_proc)] for _ in range(os.cpu_count())]
    # b = [(_*(quantity_files_per_proc),(1+_)*(quantity_files_per_proc)) for _ in range(os.cpu_count())]
    # print(b)
    # print(len(list(itertools.chain.from_iterable(a))))
    # print(len(os.listdir(INDIR)))
    # for var1,var2,*_ in itertools.zip_longest(list(itertools.chain.from_iterable(a)),os.listdir(INDIR)):
        
    #     if var1!=var2:
            # print('fdaf')
    # quantity_files_per_proc = len(os.listdir(INDIR))//os.cpu_count()
    # quantity_per_proc = [_*(quantity_files_per_proc+1) for _ in range(os.cpu_count())]
    # pass
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
    
    
      
    