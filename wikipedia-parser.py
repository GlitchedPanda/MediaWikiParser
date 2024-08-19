from xml.sax import ContentHandler, parse
from logging import getLogger
from bz2 import BZ2File

logger = getLogger("wikipedia-parser")

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

def printit(inputs):
    title,text = inputs
    print(title)
    #print(text)

if __name__ == "__main__":
    wiki = BZ2File("data.xml.bz2")
    parser = WikiParser(lambda ns: ns == 0, printit)

    parse(wiki, parser)

