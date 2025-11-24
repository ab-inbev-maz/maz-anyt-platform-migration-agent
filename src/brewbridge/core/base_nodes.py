from __future__ import annotations

from functools import wraps

from loguru import logger

from brewbridge.core.state import MigrationGraphState


def tool_node(func):
    @wraps(func)
    def wrapper(state: MigrationGraphState) -> MigrationGraphState:
        name = func.__name__
        logger.info(f"🧩 [TOOL] Running: {name}")
        try:
            out = func(state)
            logger.info(f"🟩 [TOOL] Completed: {name}")
            return out
        except Exception as e:
            logger.error(f"🟥 [TOOL] Failed: {name} | {e}")
            raise

    return wrapper


def agent_node(func):
    @wraps(func)
    def wrapper(state: MigrationGraphState) -> MigrationGraphState:
        name = func.__name__
        logger.info(f"🤖 [AGENT] Running: {name}")
        try:
            out = func(state)
            logger.info(f"🟦 [AGENT] Completed: {name}")
            return out
        except Exception as e:
            logger.error(f"🟥 [AGENT] Failed: {name} | {e}")
            raise

    return wrapper


def human_node(func):
    @wraps(func)
    def wrapper(state: MigrationGraphState) -> MigrationGraphState:
        name = func.__name__
        logger.info(f"🧍 [HUMAN] Awaiting: {name}")
        try:
            out = func(state)
            logger.info(f"🟫 [HUMAN] Completed: {name}")
            return out
        except Exception as e:
            logger.error(f"🟥 [HUMAN] Failed: {name} | {e}")
            raise

    return wrapper
