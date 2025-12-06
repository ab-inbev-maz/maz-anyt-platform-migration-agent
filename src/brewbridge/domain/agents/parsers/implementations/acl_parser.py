from ..base_parser import BaseParser
from ..factory import ParserFactory

class ACLParser(BaseParser):

    def parse(self, data: dict) -> dict:
        section = data.get("acl", {})
        return section

ParserFactory.register("acl", ACLParser)