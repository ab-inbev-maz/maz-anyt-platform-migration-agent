from ..base_parser import BaseParser
from ..factory import ParserFactory

class QualityParser(BaseParser):

    def parse(self, data: dict) -> dict:
        section = data.get("quality", {})
        return section

ParserFactory.register("quality", QualityParser)