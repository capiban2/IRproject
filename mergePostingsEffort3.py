import os
import re
from typing import Generator,Optional
import time
import multiprocessing
import shutil
import pickle
from dataclasses import dataclass


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
       

def mergeDifferentFiles(dirpath : str, finalstoragepath : str)  -> None:
    dirlist = os.listdir(dirpath)
    
    
@dataclass
class Holder:
    terms : str
    posting : list[int]
    
    # def insertTerms(self,term,docID):
    #     self.terms.append(term)
    #     self.posting.append([docID])
        
        
'''
    purpose this function is handle specific case, when quantity received files is odd
'''
def _serializeOneFile(path : str,blksize : int,outputpath : str,number : int) ->None:
    input_fd = open(path)
    last_elem_holder = ''
    with open(outputpath,'wb') as binary_output:
        while (flow:= input_fd.read(blksize).split(' ')) != ['']:
            if last_elem_holder:
                flow[0] = last_elem_holder+flow[0]
            last_elem_holder = flow[-1]
            # for _ in flow[:-1]:
            #     pickle.dump(Holder(_,[number]),binary_output)
            # tmp = b' '.join(pickle.dumps(Holder(_,[number])) for _ in flow[:-1])
            binary_output.write(b''.join(pickle.dumps(Holder(_,[number])) for _ in flow[:-1]))
        pickle.dump(Holder(last_elem_holder,[number]),binary_output)

            
            
            

def _mergeTwoDifferentFiles(f_path : str , s_path : str ,numbers : list[int],blksize : int,outputpath : str) ->None:
    filesdescrs = [open(f_path), open(s_path)]
    buffer = [filesdescrs[0].read(blksize).split(' '),filesdescrs[1].read(blksize).split(' ')]
    last_parts_maintainer = [buffer[0].pop(-1),buffer[1].pop(-1)]
    list_of_holders = []
    with open(outputpath,'wb') as binary_output:
        while True:
            try:
                if buffer[0][0] == buffer[1][0]:
                    list_of_holders.append(Holder(buffer[0][0],[numbers[0],numbers[1]]))
                    buffer[0].pop(0)
                    buffer[1].pop(0)
                    continue
                if buffer[0][0] < buffer[1][0]:
                    list_of_holders.append(Holder(buffer[0][0],[numbers[0]]))
                    buffer[0].pop(0)
                    continue
                list_of_holders.append(Holder(buffer[1][0],[numbers[1]]))
                buffer[1].pop(0)
            except IndexError:
                binary_output.write(b''.join([pickle.dumps(_) for _ in list_of_holders]))
                list_of_holders = []
                idees_to_remove = []
                for id,_ in enumerate(buffer):
                    if not len(_):
                        if (flow:= filesdescrs[id].read().split(' ')) == ['']:
                            
                            if last_parts_maintainer[id]:
                                _.append(last_parts_maintainer[id])
                                last_parts_maintainer[id] = ''
                            else:
                                idees_to_remove.append(id -len(idees_to_remove))
                                # buffer.pop(id-helper)
                                # numbers.pop(id-helper)
                                # filesdescrs[id-helper].close()
                                # # pickle.dump(Holder(last_parts_maintainer[id-helper],[numbers[id-helper]]),binary_output)
                                # helper+=1
                        else:
                            buffer[id] = flow
                            buffer[id][0] = last_parts_maintainer[id]+ flow[0]
                            last_parts_maintainer[id] = buffer[id].pop(-1)
                            continue
                for id in idees_to_remove:
                    buffer.pop(id)
                    numbers.pop(id)
                    filesdescrs[id].close()
                
                match len(buffer):
                    case 0:
                        break
                    case 2:
                        continue
                    case 1:
                        buffer[0].extend(filesdescrs[0].read().split(' '))
                        binary_output.write(b''.join([pickle.dumps(Holder(_,[numbers[0]])) for _ in buffer[0]]))
                        break
                
              
            # if len(buffer)==2:
            #     continue
            # if len(buffer) == 0:
            #     break
            
            
                    
                        
                
            
            
        
        
    
    
    

if __name__ == '__main__':
    # start = time.perf_counter()
    #mergeChunkFiles(DIRPATH,os.listdir(DIRPATH),f'{os.path.join(dir,os.path.basename(dir).split(".")[0])}.out')
    # mergeSameDir('/home/iv/Documents/mydir/kursovaya/indexbinaries/world192','/home/iv/Documents/mydir/kursovaya/indexbinaries')
    # print(time.perf_counter() - start)
    # _serializeOneFile('./testserialize.txt',1024,'./testserialize.bin',1)
    # with open('./testserialize.bin','rb') as binary_input:
    #     res = []
    #     while True:
    #         try:
    #             res.append(pickle.load(binary_input))
    #         except EOFError:
    #             break
            
    #     print(res)
    # _mergeTwoDifferentFiles('./testserialize1.txt','./testserialize2.txt',[1,2],1024,'./testserializeout.bin')
    
    # with open('./testserializeout.bin','rb') as binary_input:
    #     res = []
    #     while True:
    #         try:
    #             res.append(pickle.load(binary_input))
    #         except EOFError:
    #             break
            
    #     print(res)
    ...
            
    
    




    