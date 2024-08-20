from xml.sax import ContentHandler, parse
from logging import getLogger
from bz2 import BZ2File
from argparse import ArgumentParser
from multiprocessing import Manager, Process
from threading import Thread
from os import path
from sys import exit

logger = getLogger("wikipedia-parser")

articleQueue = None
writeQueue = None
allDataRead = None

class WikiParser(ContentHandler):
    def __init__(self, namespace_filter, callback):
        super().__init__()

        self.filter = namespace_filter
        self.callback = callback

        self.read_stack = []

        self.current_namespace = None
        self.current_title = None
        self.current_text = None

        self.pages_processed = 0

    def startElement(self, tag_name, attrs):
        if tag_name == "ns":
            self.current_namespace = None
        elif tag_name == "page":
            self.current_title = None
            self.current_text = None
        elif tag_name == "title":
            self.current_title = ""
        elif tag_name == "text":
            self.current_text = ""
        else:
            return

        self.read_stack.append(tag_name)

    def endElement(self, tag_name):
        if len(self.read_stack) > 0 and self.read_stack[-1] == tag_name:
            del self.read_stack[-1]

        if not self.filter(self.current_namespace):
            return

        if tag_name == "page" and self.current_text is not None:
            self.pages_processed += 1
            self.callback((self.current_title, self.current_text))

    def characters(self, content):
        if len(self.read_stack) <= 0 or not content:
            return

        if self.read_stack[-1] == "ns":
            ns_content = content.strip()
            self.current_namespace = int(ns_content)
        elif self.read_stack[-1] == "title":
            self.current_title += content
        elif self.read_stack[-1] == "text":
            self.current_text += content

def processArticles():
    while not (allDataRead and articleQueue.empty()):
        title,text = articleQueue.get()

        writeQueue.put(title)

def processWriting(outFile):
    while not (allDataRead and writeQueue.empty()):
        out = writeQueue.get()
        outFile.write(out + "\n")

if __name__ == "__main__":
    allDataRead = False

    argumentParser = ArgumentParser()
    argumentParser.add_argument("--processes", type=int, default=15)
    argumentParser.add_argument("data_in", help="Example: data.xml.bz2")
    argumentParser.add_argument("data_out", help="Example: database.db")
    
    arguments = argumentParser.parse_args()

    manager = Manager()
    articleQueue = manager.Queue(maxsize=2048)
    writeQueue = manager.Queue(maxsize=2048)

    wikiData = BZ2File(arguments.data_in)
    outFile = open(path.join(arguments.data_out), "a+") 
    
    processes = []
    for i in range(arguments.processes):
        process = Process(target=processArticles)
        process.start()
        processes.append(process)

    writeThread = Thread(target=processWriting, args=(outFile,))
    writeThread.start()
    
    try:
        parser = WikiParser(lambda ns: ns == 0, articleQueue.put)
        parse(wikiData, parser)

        allDataRead = True
    except KeyboardInterrupt:
        print("KeyboardInterrupt detected. Terminating...")
        
        outFile.close()
        print("Closed outFile")
        
        while not articleQueue.empty():
            articleQueue.get()
        print("Cleared articleQueue")
        for process in processes:
            process.terminate()
            process.join()
        print("Terminated processes")
        
        allDataRead = True
        while not writeQueue.empty():
            writeQueue.get()
        print("Cleared writeQueue")

        print("Exiting...")
        exit(1)

