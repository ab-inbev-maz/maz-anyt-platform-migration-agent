from abc import ABC, abstractmethod

class BaseParser(ABC):

    @abstractmethod
    def parse(self, data: dict) -> dict:
        """Extract structured information from shared JSON."""