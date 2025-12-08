from abc import ABC, abstractmethod
import yaml

class BaseTranslator(ABC):

    def __init__(self):
        super().__init__()
        self.template = None
        self.changes = None

    @abstractmethod
    def parse(self):
        pass

    def load_template(self, path : str) -> dict:
        """Loads the yaml templates for BrewDat 4.0

        Args:
            path (str): Path of the file

        Returns:
            dict: the loaded yaml
        """
        with open(path, "r") as file:
            self.template = yaml.safe_load(file)
        return self.template

    def write(self, output_path : str) -> None:
        # Change output in yaml files and add to the state
        # Probably a single key that act as a dictionary to avoid many keys in the state
        for k,v in self.changes.items():
            self.template[k] = v
        yaml.safe_dump(open(output_path, "w"), self.template)