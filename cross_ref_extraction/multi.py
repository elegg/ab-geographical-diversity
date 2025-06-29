from multiprocessing import Queue, Process, Pool, Manager
import time
import json
from extractor import paperWorker, fileSelector, paperWorker3
from enum import Enum
import gzip

"""
def worker(qs):
    inQ, outQ = qs
    while not inQ.empty():
        res = inQ.get()
        time.sleep(2)
        outQ.put(res)


        

def handler(outQ, final):

    state = 0
    while True:
        time.sleep(1)
        res = outQ.get()
        print(res, flush=True)
        if res==final:
            return 


"""

def openFile(file):
    
    with gzip.open(file, "rt") as f:
        expected_dict = json.load(f)
        return expected_dict.get("items")

    

status = Enum("Status", ["SUCCESS", "FAIL", "INSERT_ERROR"])


def page_reader(page):
    try:
        contents= openFile(page)
        return {"type":"SUCCESS", "payload":{"page":page, "papers":contents} }
    except:
       return {"type":"FAIL", "payload":{"page":page}} 
    

def readerWorker(qs):
    inQ, outQ = qs
    while not inQ.empty():
        res = inQ.get()
        try:
            contents= openFile(res)
            outQ.put( {"type":"SUCCESS", "payload":{"page":res, "papers":contents} })
        except:
            outQ.put( {"type":"FAIL", "payload":{"page":res}} )
    return

if __name__=="__main__":
    m = Manager()
    inQ = m.Queue()
    #outQ = m.Queue(20)
    t1 =time.time()
    files = fileSelector(15248,None )
    [inQ.put(i) for i in files]
    #[inQ.put("KILL") for i in range(8)]
    pool = Pool(processes=8)
    #writer = pool.apply_async(paperWorker, (files, outQ))
    res = pool.map_async(paperWorker3, [([], inQ) for i in range(8)])
    #writer.get()
    x = res.get()
    print(x)

    pool.close()

    #pool.close()
    #pool.close()
    #h = Process(target=handler, args=(outQ,))
    #h.start()
    #h.join()
    print(time.time()-t1)





