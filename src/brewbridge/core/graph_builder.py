from langgraph.graph import END, START, StateGraph

from brewbridge.core.state import MigrationGraphState
from brewbridge.domain.tools.extractor.v3.extractor_tool import extractor_node
from brewbridge.domain.tools.repo_cloner import repo_cloner
from brewbridge.domain.tools.set_up import read_manifest_and_check_api
from brewbridge.domain.tools.signal_extractor import signal_extractor_node
from brewbridge.domain.tools.template_creator import template_creator
from brewbridge.domain.tools.validator import validator
from brewbridge.infrastructure.logger import get_logger


class MigrationGraphBuilder:
    def __init__(self, logger=None):
        self.logger = logger or get_logger(__name__)
        self.nodes = {}
        self.edges = {}
        self.start_node = None

    def build(self):
        self.nodes["read_manifest_and_check_api"] = read_manifest_and_check_api
        self.nodes["repo_cloner"] = repo_cloner
        self.nodes["template_creator"] = template_creator
        self.nodes["extractor_node"] = extractor_node
        self.nodes["validator"] = validator
        self.nodes["signal_extractor_node"] = signal_extractor_node

        self.start_node = "read_manifest_and_check_api"
        self.edges = {
            START: "read_manifest_and_check_api",
            "read_manifest_and_check_api": "repo_cloner",
            "repo_cloner": "extractor_node",
            "extractor_node": "signal_extractor_node",
            "signal_extractor_node": "template_creator",
            "template_creator": "validator",
            "validator": END,
        }
        return self

    def compile(self):
        graph = StateGraph(MigrationGraphState)
        for name, func in self.nodes.items():
            graph.add_node(name, func)
        for src, dst in self.edges.items():
            graph.add_edge(src, dst)
        return graph.compile()
