from ..base_parser import BaseParser
from ..factory import ParserFactory

class MetadataParser(BaseParser):

    def parse(self, data: dict) -> dict:
        section = data.get("metadata", {})
        return section

ParserFactory.register("metadata", MetadataParser)