from ..base_parser import BaseParser
from ..factory import ParserFactory

class PipelineParser(BaseParser):

    def parse(self, data: dict) -> dict:
        section = data.get("pipeline", {})
        return section

ParserFactory.register("pipeline", PipelineParser)