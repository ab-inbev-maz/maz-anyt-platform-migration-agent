"""Definitions of the custom translators used by the migration agent
"""

from brewbridge.domain.agents.translators.base_translator import BaseTranslator

class ACLTranslator(BaseTranslator):

    def __init__(self):
        super().__init__()

    def parse(self, acl_schema : dict) -> None:
        pass
    