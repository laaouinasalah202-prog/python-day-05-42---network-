
from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional

class animal(ABC):
    @property
    @abstractmethod
    def make_sound(self):
        pass

class ani(animal):
    @property
    def make_sound(self):
        return "d().dd()"


