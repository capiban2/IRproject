from btreeeffort3 import TreeHolder,Key
from encodeASCIIletters import encodeString
from encodeDigits import packedBCD
from ProcessTXTfilesconcurrent import treatTXTfiles
from mergePostingsEffort2 import mergeSameDir1,MergeDifferentFiles

import os
import time

INPUFILESPATH='/home/iv/Documents/mydir/kursovaya/filesforindexing'
OUTPUTINDEXESPATH='/home/iv/Documents/mydir/indexbinaries'
TREESTORAGE='/home/iv/Documents/mydir/kursovaya/treestorage'

BLKSIZE = 512

def invokepuppets(infiles : str,tmp_outfile : str,blksize : int,resultindexpath : str,treestorage : str) -> None:
    treatTXTfiles(infiles,tmp_outfile)
    for dirname in os.listdir(tmp_outfile):
        mergeSameDir1(os.path.join(tmp_outfile,dirname),blksize,tmp_outfile)
    
    jungle = TreeHolder()
    offset = 0
    with open(resultindexpath,'wb') as result_index:
        for term, posting in MergeDifferentFiles(tmp_outfile):
            encoded_term = encodeString(term)
            packedPosting = packedBCD(posting) 
            jungle.insertKey(Key((offset,len(packedPosting)),encoded_term))
            result_index.write(packedPosting)
            offset+=len(packedPosting)
    jungle.storeTrees(treestorage)
    
    