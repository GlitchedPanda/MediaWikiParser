from xml.sax import ContentHandler, parse

from logging import getLogger

from bz2 import BZ2File
from sqlite3 import SQLITE_OK
from sqlite3 import connect as sqlite3_connect

from argparse import ArgumentParser

from multiprocessing import Manager, Process, Value
from threading import Thread

from os import path
from sys import exit
from time import sleep

from re import sub, escape, DOTALL, IGNORECASE

'''
TODO
1. Improve article cleaning
2. Improve shutdown
3. Improve status
'''

logger = getLogger("wikipedia-parser")

articleQueue = None
writeQueue = None
allDataRead = None

conn = None
writtenCount = 0

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

def clean_nested(text, pattern):
    while True:
        newText = sub(pattern, lambda m: m.group(1), text, flags=DOTALL)
        if newText == text:
            break
        text = newText
    return text

def processArticles():
    while not (allDataRead and articleQueue.empty()):
        title,text = articleQueue.get()
        
        # https://github.com/awslabs/fever/blob/master/fever-annotations-platform/src/dataset/reader/cleaning.py
        cleanedText = text

        # HTML Comments
        cleanedText = sub(r'(<!--.*?-->)', "", cleanedText, flags=DOTALL)
        # Refs
        cleanedText = sub(r'<ref( name ?= ?\"?(.*?)\"?)?((>(.*?)<\/ref>)|(\ ?\/>))', r'', cleanedText, flags=DOTALL)
        # Files
        cleanedText = sub(r'\[\[File(.*?)\]\]', r'', cleanedText)
        cleanedText = sub(r'\[\[Image(.*?)\]\]', r'', cleanedText)

        cleanedText = sub(r'(?i)\{\{IPA(\-[^\|\{\}]+)*?\|([^\|\{\}]+)(\|[^\{\}]+)*?\}\}', lambda m: " -LSB- "+ m.group(2)+" -RSB- ", cleanedText)
        cleanedText = sub(r'(?i)\{\{Convert\|(.*?)\|(.*?)(\|.*?)\}\}', lambda m: m.group(1)+m.group(2), cleanedText)
        cleanedText = sub(r'(?i)\[\[wikt\:(.*?)\|.*?\]\]', lambda m: m.group(1), cleanedText)

        cleanedText = sub(r'\{\{commonscat-inline|(.*?)\}\}', r'', cleanedText)
        cleanedText = clean_nested(cleanedText, r'\[\[Category:(.*?)\]\]')

        cleanedText = sub(r'\[\[([^[\]]*)\|([^[\]]*)\]\]', lambda m: m.group(2), cleanedText)

        cleanedText = clean_nested(cleanedText, r'\[\[(.*?)\]\]')
        cleanedText = clean_nested(cleanedText, r'\{\{(.*?)\}\}')
        cleanedText = clean_nested(cleanedText, r'\{\|(.*?)\|\}')

        cleanedText = clean_nested(cleanedText, r'\'\'\'(.*?)\'\'\'')
        cleanedText = clean_nested(cleanedText, r'\'\'(.*?)\'\'')

        # Post
        cleanedText = sub(r'\|', r'', cleanedText) # Some links to other pages have weird formatting. Ex: [[Boots| ]]

        cleanedText = sub(r'&nbsp;',' ',cleanedText)
        cleanedText = sub(r'<br\s?/?>','\n',cleanedText)

        cleanedText = sub(r'\{\{cite(.*?)\}\}', r'', cleanedText, flags=DOTALL + IGNORECASE)

        cleanedText = sub(r'\([^a-zA-Z0-9]*\)', ' ', cleanedText)
        cleanedText = sub(r'\([\s]*\)', ' ', cleanedText)

        writeQueue.put((title, cleanedText))

def processWriting(outFile):
    global writtenCount
    conn = sqlite3_connect(outFile)
    cursor = conn.cursor()

    cursor.execute("CREATE TABLE IF NOT EXISTS pages (title TEXT NOT NULL UNIQUE, content TEXT NOT NULL)")

    while not (allDataRead and writeQueue.empty()):
        title,text = writeQueue.get()
        cursor.execute("INSERT OR IGNORE INTO pages VALUES (?, ?)", (title, text))
        conn.commit()
        
        writtenCount += 1

# https://jamesthorne.com/blog/processing-wikipedia-in-a-couple-of-hours/
def display():
    while True:
        print("Queue sizes: articleQueue={0} writeQueue={1} Read: {2} Written: {3}".format(
            articleQueue.qsize(), 
            writeQueue.qsize(), 
            parser.pages_processed,
            writtenCount))
        sleep(1)

if __name__ == "__main__":
    allDataRead = False

    argumentParser = ArgumentParser()
    argumentParser.add_argument("--processes", type=int, default=15)
    argumentParser.add_argument("data_in", help="Example: data.xml.bz2")
    argumentParser.add_argument("data_out", help="Example: database.db")
    
    arguments = argumentParser.parse_args()

    manager = Manager()
    articleQueue = manager.Queue()
    writeQueue = manager.Queue()

    wikiData = BZ2File(arguments.data_in)
    outFile = path.join(arguments.data_out) 
    
    try:
        parser = WikiParser(lambda ns: ns == 0, articleQueue.put)

        statusThread = Thread(target=display, args=())
        statusThread.start() 

        processes = []
        for i in range(arguments.processes):
            process = Process(target=processArticles)
            process.start()
            processes.append(process)
        
        writeThread = Thread(target=processWriting, args=(outFile,))
        writeThread.start()

        parse(wikiData, parser)

        allDataRead = True
    except KeyboardInterrupt:
        print("KeyboardInterrupt detected. Terminating...")
        
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

