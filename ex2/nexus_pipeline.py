from typing import Any, List, Dict, Union, Optional, Protocol
from abc import ABC, abstractmethod


class ProcessingPipeline(ABC):
    ...

class ProcessingStage(Protocol):
        
        def process(self, data: Any) -> Any:
            ...

class InputStage(ProcessingStage):

        def process(self, data: Any) -> Any:
            ...

class TransformStage(ProcessingStage):
        ...
        def process(self, data: Any) -> Any:
            ...

class OutputStage(ProcessingStage):

        def process(self, data: Any) -> Any:
            ...

class JSONAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id):
        self.pipeline_id = pipeline_id
    
class CSVAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id):
        self.pipeline_id = pipeline_id


class StreamAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id):
        self.pipeline_id = pipeline_id

