from ..base_parser import BaseParser
from ..factory import ParserFactory

class ObservabilityParser(BaseParser):

    def parse(self, data: dict) -> dict:
        section = data.get("observability", {})
        return section

ParserFactory.register("observability", ObservabilityParser)