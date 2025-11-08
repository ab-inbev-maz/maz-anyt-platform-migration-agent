# src/migration_agent/core/base_nodes.py

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from loguru import logger
from migration_agent.core.state import MigrationGraphState


class BaseNode(ABC):
    """
    Abstract base class for all nodes in the BrewBridge AI migration graph.

    Each node (tool, agent, or human) receives a `MigrationGraphState` object,
    performs a specific operation, and returns an updated state.

    Responsibilities:
      - Standardize logging and exception handling
      - Define the run() interface
      - Enable composability inside LangGraph workflows
    """

    node_name: str

    def __init__(self, node_name: Optional[str] = None) -> None:
        self.node_name = node_name or self.__class__.__name__

    @abstractmethod
    def run(self, state: MigrationGraphState) -> MigrationGraphState:
        """Execute the node logic and return the updated state."""
        raise NotImplementedError

    def safe_run(self, state: MigrationGraphState) -> MigrationGraphState:
        """
        Execute node logic with logging and error handling.
        This wrapper is used when running nodes inside LangGraph workflows.
        """
        logger.info(f"🧩 Running node: {self.node_name}")
        try:
            updated_state = self.run(state)
            logger.info(f"✅ Node {self.node_name} completed successfully.")
            return updated_state
        except Exception as e:
            logger.error(f"❌ Node {self.node_name} failed: {e}")
            raise


class ToolNode(BaseNode):
    """
    Deterministic node type — executes pure Python logic (no LLM interaction).

    Examples:
      - ManifestReader
      - FrameworkCreator
      - ExtractorTool
      - ValidatorTool
      - Generator
    """

    def __init__(self, node_name: Optional[str] = None) -> None:
        super().__init__(node_name=node_name)

    @abstractmethod
    def execute(self, state: MigrationGraphState) -> MigrationGraphState:
        """Concrete tools must implement this deterministic function."""
        raise NotImplementedError

    def run(self, state: MigrationGraphState) -> MigrationGraphState:
        """Wrapper around execute() with error handling."""
        return self.execute(state)


class AgentNode(BaseNode):
    """
    LLM-powered node type — executes intelligent transformations using an LLM.

    Examples:
      - SchemaNormalizer
      - CorrectorAgent
      - ReporterLogger
      - Translators (ACL, Metadata, Pipeline, etc.)
    """

    model_name: str
    prompt_template: str

    def __init__(
        self,
        model_name: str,
        prompt_template: str,
        node_name: Optional[str] = None,
    ) -> None:
        super().__init__(node_name=node_name)
        self.model_name = model_name
        self.prompt_template = prompt_template

    @abstractmethod
    def call_llm(self, input_data: Dict[str, Any]) -> str:
        """Perform the actual LLM call and return its textual output."""
        raise NotImplementedError

    @abstractmethod
    def prepare_prompt(self, state: MigrationGraphState) -> Dict[str, Any]:
        """Prepare structured input data for the LLM call."""
        raise NotImplementedError

    def run(self, state: MigrationGraphState) -> MigrationGraphState:
        """Orchestrate the prompt preparation, LLM call, and state update."""
        logger.info(f"🤖 Running LLM agent node: {self.node_name}")
        input_data = self.prepare_prompt(state)
        output_text = self.call_llm(input_data)
        return self.update_state(state, output_text)

    @abstractmethod
    def update_state(self, state: MigrationGraphState, llm_output: str) -> MigrationGraphState:
        """Update the state with the LLM-generated output."""
        raise NotImplementedError


class HumanNode(BaseNode):
    """
    Human-in-the-loop node type — pauses execution and waits for manual input.

    Examples:
      - HumanApprovalNode
      - HumanDecisionCheck
    """

    prompt_message: str

    def __init__(self, prompt_message: str, node_name: Optional[str] = None) -> None:
        super().__init__(node_name=node_name)
        self.prompt_message = prompt_message

    def run(self, state: MigrationGraphState) -> MigrationGraphState:
        """
        Pause execution and wait for human input.
        In production, this would be integrated with a UI or messaging system.
        """
        logger.info(f"🧍 Human node '{self.node_name}' awaiting input: {self.prompt_message}")

        decision = state.human_approval_decision
        if decision is None:
            logger.warning("Human decision not provided yet. Execution paused.")
            raise RuntimeError(
                f"Node '{self.node_name}' paused: waiting for human_approval_decision in state."
            )

        logger.info(f"Human decision received: {decision}")
        return state
