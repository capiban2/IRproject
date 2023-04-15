from mergepostrings import MergePostings
import os
import multiprocessing
import time


def printInfo(val):
    print(os.getpid())


class MergerSameFile:
    def __init__(self,dirname:str,chunksize:int,offset:int,blocksize):
        self.dirname = dirname
        self.start_with_file_with_number = offset
        self.quantity_files = chunksize
        self.bk = blocksize
    def __iter__(self):
        self.filedescriptors = [open(os.path.join(self.dirname,filename)) for filename in os.listdir(self.dirname)[self.start_with_file_with_number:self.start_with_file_with_number+self.quantity_files]]
        self.buffer = [fd.read(self.bk).split(' ') for fd in self.filedescriptors]
        self.queue = {}
        
        return self 
    '''
        files for merging have names like \d+.txt and result files will be have names like resultfile\d+.txt or smth like that.
        (this information neccesary to properly select files for merging)
        |---dir
        |  |--0.txt
        |  |--1.txt.
        ---------
        |  |---resfilesdir
        |    |--
        |    |--resultfile1.txt
        |    |--resultfile2.txt
        -------------------------
    
    '''
    
    def __next__(self):
        is_close = False
        
    #def mergeChunkOfFiles(self,quantity:int,startwith:int):
        


if __name__ == '__main__':
    start = time.perf_counter()
    with multiprocessing.Pool(processes=4) as mp:
       mp.map(
           printInfo, (val for val in range(os.cpu_count()))
       )
    print(time.perf_counter()-start)
      
    