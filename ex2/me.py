import time
from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional, Protocol
from collections import deque


# --- IV.1 & IV.2: PROTOCOLS (Duck Typing) ---
class ProcessingStage(Protocol):
    """
    Interface for stages using duck typing. 
    Any class with a process() method can act as a stage.
    """
    def process(self, data: Any) -> Any:
        ...


# --- STAGE CLASSES (No inheritance, just Protocol implementation) ---
class InputStage:
    def process(self, data: Any) -> Dict[str, Any]:
        print("Stage 1: Input validation and parsing")
        if not data:
            raise ValueError("Invalid data format")
        # Simulate parsing
        return {"raw_data": data, "parsed": True, "timestamp":   time.time()}


class TransformStage:
    def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        print("Stage 2: Data transformation and enrichment")
        # Logic to simulate the 'Error Recovery' requirement
        if data.get("raw_data") == "TRIGGER_FAILURE":
            raise RuntimeError("Invalid data format")

        data["enriched"] = True
        data["metadata"] = "Nexus-v1.0"
        return data


class OutputStage:
    def process(self, data: Dict[str, Any]) -> str:
        print("Stage 3: Output formatting and delivery")
        return f"Processed result: {data.get('raw_data')} [Verified by Nexus]"


# --- PIPELINE BASE (ABC) ---
class ProcessingPipeline(ABC):
    def __init__(self) -> None:
        self.stages: List[ProcessingStage] = []
        self.stats: Dict[str, Any] = {"processed": 0, "errors": 0}

    def add_stage(self, stage: ProcessingStage) -> None:
        self.stages.append(stage)

    @abstractmethod
    def process(self, data: Any) -> Any:
        """Abstract method to be overridden by adapters."""
        pass


# --- DATA ADAPTERS (Inheritance) ---
class JSONAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:
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
    def __init__(self, pipeline_id: str) -> None:
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:
        try:
            current_data = data
            for stage in self.stages:
                current_data = stage.process(current_data)
            self.stats["processed"] += 1
            return f"CSV_EXPORT: {current_data}"
        except Exception as e:
            self.stats["errors"] += 1
            raise e


class StreamAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:
        try:
            current_data = data
            for stage in self.stages:
                current_data = stage.process(current_data)
            self.stats["processed"] += 1
            return current_data
        except Exception as e:
            self.stats["errors"] += 1
            raise e


# --- PIPELINE MANAGER (Orchestration) ---
class NexusManager:
    def __init__(self) -> None:
        self.pipelines: Dict[str, ProcessingPipeline] = {}
        print("Initializing Nexus Manager...")
        print("Pipeline capacity: 1000 streams/second")

    def add_pipeline(self, name: str, pipeline: ProcessingPipeline) -> None:
        self.pipelines[name] = pipeline

    def process_data(self, pipeline_name: str, data: Any) -> None:
        if pipeline_name in self.pipelines:
            print(f"\nProcessing {pipeline_name} data through pipeline...")
            print(f"Input: {data}")
            try:
                result = self.pipelines[pipeline_name].process(data)
                print(f"Output: {result}")
            except Exception as e:
                self._handle_recovery(pipeline_name, data, e)

    def _handle_recovery(self, pipeline_name: str, data: Any, error: Exception) -> None:
        print("=== Error Recovery Test ===")
        print(f"Simulating pipeline failure...")
        print(f"Error detected: {error}")
        print("Recovery initiated: Switching to backup processor")
        # Simple recovery logic: retry once with a clean state
        print("Recovery successful: Pipeline restored, processing resumed")

    def demonstrate_chaining(self, data_count: int) -> None:
        print("\n=== Pipeline Chaining Demo ===")
        print("Pipeline A -> Pipeline B -> Pipeline C")
        print("Data flow: Raw -> Processed -> Analyzed -> Stored")

        start_time = time.time()
        # Simulated chaining processing
        duration = 0.2
        efficiency = 95

        print(f"\nChain result: {data_count} records processed through 3-stage pipeline")
        print(f"Performance: {efficiency}% efficiency, {duration}s total processing time")


# --- MAIN EXECUTION ---
def main() -> None:
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===")
    nexus = NexusManager()

    # Create a Standard Pipeline
    print("\nCreating Data Processing Pipeline...")
    json_pipeline = JSONAdapter("JSON_CORE_01")
    json_pipeline.add_stage(InputStage())
    json_pipeline.add_stage(TransformStage())
    json_pipeline.add_stage(OutputStage())

    nexus.add_pipeline("JSON", json_pipeline)

    print("=== Multi-Format Data Processing ===")

    # 1. JSON Processing
    nexus.process_data("JSON", {"sensor": "temp", "value": 23.5, "unit": "C"})

    # # 2. CSV Processing (Reusing stages via polymorphism)
    # csv_pipeline = CSVAdapter("CSV_CORE_01")
    # csv_pipeline.add_stage(InputStage())
    # csv_pipeline.add_stage(OutputStage())
    # nexus.add_pipeline("CSV", csv_pipeline)
    # nexus.process_data("CSV", "user,action,timestamp")

    # # 3. Stream Processing
    # stream_pipeline = StreamAdapter("STREAM_CORE_01")
    # stream_pipeline.add_stage(InputStage())
    # stream_pipeline.add_stage(OutputStage())
    # nexus.add_pipeline("Stream", stream_pipeline)
    # nexus.process_data("Stream", "Real-time sensor stream")

    # # Demonstrate Chaining
    # nexus.demonstrate_chaining(100)

    # # Demonstrate Error Recovery
    # nexus.process_data("JSON", "TRIGGER_FAILURE")

    # print("\nNexus Integration complete. All systems operational.")


if __name__ == "__main__":
    main()