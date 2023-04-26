import os
import re
from typing import Generator,Optional
import time
import multiprocessing
import shutil
import pickle
from dataclasses import dataclass
from typing import Any
from copy import deepcopy
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
       

def mergeDifferentFiles(dirpath : str,blksize:int)  -> None:
    dirlist = os.listdir(dirpath)
    
    with multiprocessing.Pool() as mp:
        mp.starmap(
            _mergeTwoDifferentFiles,
            ((os.path.join(dirpath,dirlist[id]),os.path.join(dirpath,dirlist[id+1]),[id,id+1],\
                blksize,os.path.join(dirpath,f'{id}{id+1}index.out')) for id in range(0,len(dirlist)-1,2))
        )
    if len(dirlist)%2!=0:
        _serializeOneFile(os.path.join(dirpath,dirlist[-1]),blksize,os.path.join(dirpath,f'{len(dirlist)}index.out'),len(dirlist))
    for _ in dirlist:
        os.remove(os.path.join(dirpath,_))
    
    
    dirlist = os.listdir(dirpath)
    if len(dirlist)<=3:
        if len(dirlist) == 3:
        
            mergeChunkHolders(dirpath,[dirlist[0],dirlist[1]],blksize)
        return
                
    if len(dirlist)>=8: 
        workers_quantity = os.cpu_count() if len(dirlist)> os.cpu_count()*2 else len(dirlist)//2
        
        files_per_proc = len(dirlist)//workers_quantity
        
        
        with multiprocessing.Pool() as mp:
            mp.starmap(
                mergeChunkHolders, 
                ((dirpath,dirlist[_*files_per_proc : (1+_)*files_per_proc],blksize) for _ in range(workers_quantity))
            )
        for _ in dirlist:
            os.remove(os.path.join(dirpath,_))
            
            
        dirlist = os.listdir(dirpath)
    files_per_proc = len(dirlist)//2
    with multiprocessing.Pool(processes=2) as mp:
        mp.starmap(
            mergeChunkHolders,
            ((dirpath,dirlist[0:files_per_proc],blksize),(dirpath,dirlist[files_per_proc:],blksize))
        )
    for _ in dirlist:
        os.remove(os.path.join(dirpath,_))
        
def finalMerger(dirpath,chunk_of_files : list[str],blksize) ->Generator[tuple[str,list[int]],None,None]:
    filedesc = [open(os.path.join(dirpath,chunk_of_files[0]),'rb'),open(os.path.join(dirpath,chunk_of_files[1]),'rb')]
    buffer = [[],[]]
    
    while True:
        try:
            if buffer[0][0].term == buffer[1][0].term:
                    
                buffer[0].pop(0)
                buffer[1].pop(0)
                yield (buffer[0][0].term,_mergeList(buffer[0][0].posting,buffer[1][0].posting))
                continue
            
            cur_number = 0 if buffer[0][0] < buffer[1][0].term else 1
                
            buffer[cur_number].pop(0)
            yield (buffer[cur_number][0].term,buffer[cur_number].posting)
            
        except IndexError:
            idees_to_remove = []
            for id,_ in enumerate(buffer): 
                if not len(_):
                    tmp = []
                    for _ in range(blksize):
                        try:
                            tmp.append(pickle.load(filedesc[id]))
                        except EOFError:
                            if tmp == []:
                                idees_to_remove.append(id-len(idees_to_remove))
                            break
                    
                    buffer[id].extend(tmp)
            for id in idees_to_remove:
                buffer.pop(id)
            
                filedesc[id].close()
                filedesc.pop(id) 
            match len(buffer):
                    case 0:
                        break
                    case 2:
                        continue
                    case 1:
                        output_record = []
                        while True:
                            try:
                                output_record.append(pickle.load(filedesc[0]))
                            except EOFError:
                                filedesc[0].close()
                                for _ in output_record:
                                    yield (_.term,_.posting)  
                                # binary_output.write(b''.join(pickle.dumps(_,5)) for _ in output_record)
                                break 
    
@dataclass
class Holder:
    term : str
    posting : list[int]
    
    # def insertterm(self,term,docID):
    #     self.term.append(term)
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
                    filesdescrs.pop(id)
                    last_parts_maintainer.pop(id)
                
                match len(buffer):
                    case 0:
                        break
                    case 2:
                        continue
                    case 1:
                        if (tmp:=filesdescrs[0].read()) != '':
                            
                            buffer[0].extend(tmp.split(' '))   
                        buffer[0] +=[last_parts_maintainer[0]]
                        filesdescrs[0].close()
                        binary_output.write(b''.join([pickle.dumps(Holder(_,[numbers[0]])) for _ in buffer[0]]))
                        break
                
              
            # if len(buffer)==2:
            #     continue
            # if len(buffer) == 0:
            #     break
            
            
def mergeFileHolders(f_path : str, s_path : str, blksize : int , outputpath : str,gen : int = 0) ->str:
    filedesc = [open(f_path,'rb'),open(s_path,'rb')]
    buffer=[[],[]]
    # while True:
    #     try:
            
    #     except EOFError:
    #         break
    # buffer = [[pickle.load(filedesc[0]) for _ in range(blksize)],[pickle.load(filedesc[1]) for _ in range(blksize)]]
    list_of_holders = []
    with open(os.path.join(outputpath,f'{gen}{os.getpid()}.out'),'wb') as binary_output:
        while True:
            try:
                if buffer[0][0].term == buffer[1][0].term:
                    list_of_holders.append(Holder(buffer[0][0].term,\
                        _mergeList(buffer[0][0].posting,buffer[1][0].posting)))
                    buffer[0].pop(0)
                    buffer[1].pop(0)
                elif buffer[0][0] < buffer[1][0].term:
                    list_of_holders.append(buffer[0][0])
                    buffer[0].pop(0)
                else:
                    list_of_holders.append(buffer[1][0])
                    buffer[1].pop(0)
                    
            except IndexError:
                binary_output.write(b''.join([pickle.dumps(_) for _ in list_of_holders]))
                list_of_holders = []
                idees_to_remove = []
                for id,_ in enumerate(buffer):
                    if not len(_):
                        tmp = []
                        for _ in range(blksize):
                            try:
                                tmp.append(pickle.load(filedesc[id]))
                            except EOFError:
                                if tmp == []:
                                    idees_to_remove.append(id-len(idees_to_remove))
                                break
                        
                        buffer[id].extend(tmp)
                for id in idees_to_remove:
                    buffer.pop(id)
                
                    filedesc[id].close()
                    filedesc.pop(id)
                match len(buffer):
                    case 0:
                        break
                    case 2:
                        continue
                    case 1:
                        output_record = []
                        while True:
                            try:
                                output_record.append(pickle.load(filedesc[0]))
                            except EOFError:
                                filedesc[0].close()
                                binary_output.write(b''.join(pickle.dumps(_,5)) for _ in output_record)
                                break
                    
    return f'{gen}{os.getpid()}.out' 

                                

def mergeChunkHolders(dirpath : str, chunk_of_holders : list[str],blksize : int)->None:
    while len(chunk_of_holders) > 1:
        merged = mergeFileHolders(os.path.join(dirpath,chunk_of_holders[0]),\
            os.path.join(dirpath,chunk_of_holders[1]),blksize,dirpath,len(chunk_of_holders))
        for _ in chunk_of_holders[:2]:
            os.remove(os.path.join(dirpath,_))
        chunk_of_holders.pop(0)
        chunk_of_holders[0] = merged
    
    
        
                        
                
            
def _mergeList(f_list : list[Any],s_list: list[Any]) ->list[Any]:
    res = deepcopy(f_list)
    
    for elem in s_list:
        if elem > res[-1]:
            res.append(elem)
        elif elem < res[0]:
            res.insert(0,elem)
        else:
            for id,_ in enumerate(res):
                if _ == elem:
                    break
                if id>0 and res[id-1] < elem < res[id]:
                    res.insert(id,elem)
                    break
        
                
        
        
        
    return res
    
    
    

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
    _mergeTwoDifferentFiles('./testserialize1.txt','./testserialize2.txt',[1,2],1024,'./testserializeout.bin')
    _mergeTwoDifferentFiles('./testserialize1.txt','./testserialize2.txt',[3,4],1024,'./testserializeout2.bin')
    mergeFileHolders('./testserializeout.bin','./testserializeout2.bin',128,'./testserializeholders.bin')
    
    
    # with open('./testserializeout.bin','rb') as binary_input:
        
    #     res = []
    #     for _ in range(1000):
    #         try:
            
    #             res.append(pickle.load(binary_input))
    #         except EOFError:
    #             break
            
    #     print(res)
    
    
            
    # a = _mergeList([1,2,4],[1,2,3,5,6,7]) 
    # assert _mergeList([1],[2]) == [1,2]
    # assert _mergeList([1,2,3],[4,5,6]) == [1,2,3,4,5,6]
    # assert _mergeList([1,5,8],[3,4,6,7]) == [1,3,4,5,6,7,8]
    
    
    # a = [_ for _ in range(0,1000,2)]
    # b = [_ for _ in range(1,1000,2)]
    # start = time.perf_counter()
    # res = _mergeList(a,b)
    # print(time.perf_counter() - start)
    
    
    
    ...



    