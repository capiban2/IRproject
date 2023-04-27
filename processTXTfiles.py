import os
import re
import time
import multiprocessing
BLOCKSIZE = 1024*8

OUTPUTPATH = './indexbinaries/'
INPUTPATH = './filesfortests2'

def processFile(path : str, outputpath : str,bksize : int):
    
    with open(path) as txt_input:
        tail = ''
        filenumber = 0
    #     while True:
    #         try:
    #             flow=txt_input.read(bksize)
    #         except UnicodeDecodeError:
    #             print(f'{path} is corrupted!!')
    #         else:
    #             if flow == '':
    #                 break
        while(flow:=txt_input.read(bksize))!='':
            
            dictionary = []
            
            clear_flow = re.findall('\d+|\w+',flow)
            
            if tail:
                clear_flow[0] = tail+clear_flow[0]
            tail = clear_flow[-1]
            clear_flow.pop(-1)
             
            
            for _ in clear_flow:
                
                    
                
                if _.lower() not in dictionary:
                    dictionary.append(_.lower())
            with open(os.path.join(outputpath,f'{filenumber}'+os.path.basename(path).split('.')[0] + '.txt'),'w') as output_txt:
                output_txt.write(' '.join(sorted(dictionary)))
            filenumber+=1
        
        with open(os.path.join(outputpath,f'{filenumber}'+os.path.basename(path).split('.')[0] + '.txt'),'w') as output_txt:
            output_txt.write(tail.lower())    
            
            
def handleTXTfiles(dirpath : str, outpath : str , blksize : int)->None:
    dirlist = os.listdir(dirpath)
    for dirname in dirlist:
        os.mkdir(os.path.join(outpath,dirname.split('.')[0]))
    workers_quantity = os.cpu_count() if len(dirlist) >= os.cpu_count() else len(dirlist)
    with multiprocessing.Pool(processes=workers_quantity) as mp:
        mp.starmap(
            processFile,
            ((os.path.join(dirpath,_),os.path.join(outpath,_.split('.')[0]),blksize) for _ in dirlist)
        )
    
    
if __name__ == '__main__':
    start = time.perf_counter()
    handleTXTfiles(INPUTPATH,OUTPUTPATH,BLOCKSIZE)   
    print(time.perf_counter() - start)
                
            
            
            