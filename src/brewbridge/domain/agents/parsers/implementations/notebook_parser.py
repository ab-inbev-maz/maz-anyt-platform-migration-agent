from ..base_parser import BaseParser
from ..factory import ParserFactory

class NotebookParser(BaseParser):

    def parse(self, data: dict) -> dict:
        section = data.get("notebook", {})
        return section

ParserFactory.register("notebook", NotebookParser)