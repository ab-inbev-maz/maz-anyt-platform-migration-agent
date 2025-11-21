from langgraph.graph import END, START, StateGraph

from brewbridge.core.state import MigrationGraphState
from brewbridge.domain.tools.template_creator import template_creator_node
from brewbridge.infrastructure.logger import get_logger


class MigrationGraphBuilder:
    def __init__(self, logger=None):
        self.logger = logger or get_logger(__name__)
        self.nodes = {}
        self.edges = {}
        self.start_node = None

    def build(self):
        self.nodes["template_creator"] = template_creator_node
        self.start_node = "template_creator"
        self.edges = {
            START: "template_creator",
            "template_creator": END,
        }
        return self

    def compile(self):
        graph = StateGraph(MigrationGraphState)
        for name, func in self.nodes.items():
            graph.add_node(name, func)
        for src, dst in self.edges.items():
            graph.add_edge(src, dst)
        return graph.compile()
