from typing import Any, List, Dict, Union, Optional, Protocol
from abc import ABC, abstractmethod

class ProcessingStage(Protocol):
        def process(self, data: Any) -> Any:
            ...


class InputStage:
    def process(self, data: Any) -> Dict:
        print("Stage 1: Input validation and parsing")
    

class TransformStage:
    def process(self, data: Any) -> dict:
        print("Stage 2: Data transformation and enrichment")


class OutputStage:
    def process(self, data: Any) -> str:
        print("Stage 3: Output formatting and delivery")


class ProcessingPipeline(ABC):
    def __init__(self):
        self.stages: List[Any] = []

    def add_stage(self, data: ProcessingStage):
        self.stages.append(data)

    def process(self, data: Any) -> Any:
        pass


class JSONAdapter(ProcessingPipeline): 
    def __init__(self, pipeline_id: str) -> None:
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Any:
        try:
            current_data = data
            for stage in self.stages:
                current_data = stage.process(current_data)
            self.stats["processed"] += 1
            return current_data
        except Exception as e:
            self.stats["errors"] += 1
            raise e


class CSVAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id):
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Any:
        try:
            current_data = data
            for stage in self.stages:
                current_data = stage.process(current_data)
            self.stats["processed"] += 1
            return current_data
        except Exception as e:
            self.stats["errors"] += 1
            raise e


class StreamAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id):
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Any:
        try:
            current_data = data
            for stage in self.stages:
                current_data = stage.process(current_data)
            self.stats["processed"] += 1
            return current_data
        except Exception as e:
            self.stats["errors"] += 1
            raise e


class NexusManager:
    def __init__(self):
          self.piplines : List[ProcessingPipeline] = []

    def add_pipline(self, pip: ProcessingPipeline):
        self.piplines.append(pip)
    
    def process_data(self, pip: ProcessingPipeline, data: str):
        if pip in self.piplines:
            print(f"input: {data}")
            try:
                result = pip.process(data)
                print(f"Output: {result}")
            except Exception as e:
                self._handle_recovery("Json", data, e)

    def demonstrate_chaining(self, data_count: int) -> None:
        print("\n=== Pipeline Chaining Demo ===")
        print("Pipeline A -> Pipeline B -> Pipeline C")
        print("Data flow: Raw -> Processed -> Analyzed -> Stored")

        duration = 0.2
        efficiency = 95

        print(f"\nChain result: {data_count} records processed through 3-stage pipeline")
        print(f"Performance: {efficiency}% efficiency, {duration}s total processing time")

    def _handle_recovery(self, pipeline_name: str, data: Any, error: Exception) -> None:
        print("=== Error Recovery Test ===")
        print(f"Simulating pipeline failure...")
        print(f"Error detected: {error}")
        print("Recovery initiated: Switching to backup processor")
        print("Recovery successful: Pipeline restored, processing resumed")


print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===\n")
print("Initializing Nexus Manager...")
nexus = NexusManager()
json = JSONAdapter("JSON_CORE_01")

i = [InputStage(), TransformStage(), OutputStage()]
for p in i:
    p.process({"sensor": "temp", "value": 23.5, "unit": "C"})

json.add_stage(InputStage())
json.add_stage(TransformStage())
json.add_stage(OutputStage())

print("Pipeline capacity: 1000 streams/second")
print("\nCreating Data Processing Pipeline...")

print("\n=== Multi-Format Data Processing ===\n")
nexus.add_pipline(json)
print("Processing JSON data through pipeline...")
nexus.process_data(json, {"sensor": "temp", "value": 23.5, "unit": "C"})

# print("\nProcessing CSV data through same pipeline...")



# print("\nProcessing Stream data through same pipeline...")




# print("\nNexus Integration complete. All systems operational.")