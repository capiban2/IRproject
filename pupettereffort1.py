from btreeeffort3 import TreeHolder,Key
from encodeASCIIletters import encodeString
from encodeDigits import packedBCD
from ProcessTXTfilesconcurrent import treatTXTfiles
from mergePostingsEffort2 import mergeSameDir1,mergeSameDirs,MergeDifferentFiles

import os
import time

INPUFILESPATH='/home/iv/Documents/mydir/kursovaya/filesfortests2'
OUTPUTINDEXESPATH='/home/iv/Documents/mydir/kursovaya/indexbinaries'
TREESTORAGE='/home/iv/Documents/mydir/kursovaya/treestorage'
RESULTINDEXSTORAGE='/home/iv/Documents/mydir/kursovaya/resultindexstorage'
BLKSIZE = 512

def invokepuppets(infiles : str,tmp_outfile : str,blksize : int,resultindexdir : str,treestorage : str) -> None:
    treatTXTfiles(infiles,tmp_outfile)
    # for dirname in os.listdir(tmp_outfile):
    #     mergeSameDir1(os.path.join(tmp_outfile,dirname),blksize,tmp_outfile)
    mergeSameDirs([os.path.join(tmp_outfile, _) for _ in os.listdir(tmp_outfile)],blksize,tmp_outfile)
            
    jungle = TreeHolder()
    offset = 0
    with open(os.path.join(resultindexdir,'index.bin'),'wb') as result_index:
        for term, posting in MergeDifferentFiles(tmp_outfile):
            encoded_term = encodeString(term)
            packedPosting = packedBCD(posting) 
            jungle.insertKey(Key((offset,len(packedPosting)),encoded_term))
            result_index.write(packedPosting)
            offset+=len(packedPosting)
    jungle.storeTrees(treestorage)
    
    
if __name__ == '__main__':
    invokepuppets(INPUFILESPATH,OUTPUTINDEXESPATH,BLKSIZE,RESULTINDEXSTORAGE,TREESTORAGE)
    
    