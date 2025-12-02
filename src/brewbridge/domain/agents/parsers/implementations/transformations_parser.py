from ..base_parser import BaseParser
from ..factory import ParserFactory

class TransformationsParser(BaseParser):

    def parse(self, data: dict) -> dict:
        section = data.get("transformations", {})
        return section

ParserFactory.register("transformations", TransformationsParser)