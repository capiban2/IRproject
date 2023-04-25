import os
import re
from typing import Generator,Optional
import time
import multiprocessing
import shutil


DIRPATH = '/home/iv/Documents/mydir/kursovaya/indexbinaries/world192'

def _mergeTwoFiles(f_list : list[str],s_list : list[str]) ->list[str]:
    tmp = set(f_list)
    for elem in s_list:
        tmp.add(elem)
        
    return sorted(list(tmp))



def mergeChunkFiles(dirpath : str, chunk_of_files : list[str] | Generator[str,None,None],outputfile : str) -> None:

    
    fd = [open(os.path.join(dirpath,_)) for _ in chunk_of_files]
    merged = _mergeTwoFiles(fd[0].read().split( ), fd[1].read().split(' '))
    for filedescr in fd[2:]:
        merged = _mergeTwoFiles(merged,filedescr.read().split(' '))
    for _ in fd:
        _.close() 
    
    with open(outputfile,'w') as output:
        output.write(' '.join(merged))
    
        
def mergeSameDir(dirpath : str, finalstoragepath : str) ->None:
    dirlist = os.listdir(dirpath)
    quantity_files_per_proc = quantity_files_per_proc = len(dirlist)//os.cpu_count()+1
    files_ranges = (dirlist[_*(quantity_files_per_proc):(1+_)*(quantity_files_per_proc)] for _ in range(os.cpu_count()))
   
    
    with multiprocessing.Pool() as mp:
        mp.starmap(
            mergeChunkFiles,
            ((dirpath,_,os.path.join(dirpath,f'{id}result.out')) for id,_ in enumerate(files_ranges))
            
        )
        
        
    
    for filename in dirlist:
        os.remove(os.path.join(dirpath,filename))
    
    mergeChunkFiles(dirpath,os.listdir(dirpath),os.path.join(dirpath,'output.out'))
    os.rename(os.path.join(dirpath,'output.out'),f'{os.path.join(finalstoragepath,os.path.basename(dirpath))}.out')
    shutil.rmtree(dirpath) 
       
       

 
        

if __name__ == '__main__':
    start = time.perf_counter()
    #mergeChunkFiles(DIRPATH,os.listdir(DIRPATH),f'{os.path.join(dir,os.path.basename(dir).split(".")[0])}.out')
    mergeSameDir('/home/iv/Documents/mydir/kursovaya/indexbinaries/world192','/home/iv/Documents/mydir/kursovaya/indexbinaries')
    print(time.perf_counter() - start)
    




    