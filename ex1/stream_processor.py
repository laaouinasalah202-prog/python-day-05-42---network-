from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional

class DataStream(ABC):
    def __init__(self, stream_id: str, stream_type: str):
        self.stream_id = stream_id
        self.stream_type = stream_type
        self.total_processed = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        if criteria is None:
            return data_batch
        return [item for item in data_batch if criteria in str(item)]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "stream_id": self.stream_id,
            "type": self.stream_type,
            "total_processed": self.total_processed
        }


class SensorStream(DataStream):
    def __init__(self, stream_id: str):
        super().__init__(stream_id, "Environmental Data")

    def process_batch(self, data_batch: List[Any]) -> str:
        readings = [v for v in data_batch if isinstance(v, (int, float))]
        self.total_processed += len(readings)

        avg = sum(readings) / len(readings) if readings else 0
        return (
            f"Sensor analysis: {len(readings)} readings processed, "
            f"avg value: {avg:.2f}"
        )


class TransactionStream(DataStream):
    def __init__(self, stream_id: str):
        super().__init__(stream_id, "Financial Data")

    def process_batch(self, data_batch: List[Any]) -> str:
        net_flow = 0
        for item in data_batch:
            if isinstance(item, dict):
                net_flow += item.get("buy", 0)
                net_flow -= item.get("sell", 0)

        self.total_processed += len(data_batch)
        sign = "+" if net_flow >= 0 else ""
        return (
            f"Transaction analysis: {len(data_batch)} operations, "
            f"net flow: {sign}{net_flow} units"
        )


class EventStream(DataStream):
    def __init__(self, stream_id: str):
        super().__init__(stream_id, "System Events")

    def process_batch(self, data_batch: List[Any]) -> str:
        errors = [e for e in data_batch if "error" in str(e).lower()]
        self.total_processed += len(data_batch)

        return (
            f"Event analysis: {len(data_batch)} events, "
            f"{len(errors)} error detected"
        )


class StreamProcessor:
    def __init__(self):
        self.streams: List[DataStream] = []

    def register_stream(self, stream: DataStream):
        self.streams.append(stream)

    def process_all(self, batches: Dict[str, List[Any]]):
        print("=== Polymorphic Stream Processing ===")
        for stream in self.streams:
            try:
                batch = batches.get(stream.stream_id, [])
                result = stream.process_batch(batch)
                print(f"- {stream.stream_type}: {result}")
            except Exception as e:
                print(f"Error processing {stream.stream_id}: {e}")


if __name__ == "__main__":
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")

    sensor = SensorStream("SENSOR_001")
    print(f"Initializing Sensor Stream...\nStream ID: {sensor.stream_id}, Type: {sensor.stream_type}")
    print(sensor.process_batch([22.5, 23.0, 21.8]))

    transaction = TransactionStream("TRANS_001")
    print(f"\nInitializing Transaction Stream...\nStream ID: {transaction.stream_id}, Type: {transaction.stream_type}")
    print(transaction.process_batch([
        {"buy": 100}, {"sell": 150}, {"buy": 75}
    ]))

    event = EventStream("EVENT_001")
    print(f"\nInitializing Event Stream...\nStream ID: {event.stream_id}, Type: {event.stream_type}")
    print(event.process_batch(["login", "error", "logout"]))

    processor = StreamProcessor()
    processor.register_stream(sensor)
    processor.register_stream(transaction)
    processor.register_stream(event)

    mixed_batches = {
        "SENSOR_001": [20.1, 19.8],
        "TRANS_001": [{"buy": 200}, {"sell": 50}],
        "EVENT_001": ["start", "error", "shutdown"]
    }

    print("\nBatch 1 Results:")
    processor.process_all(mixed_batches)

    print("\nAll streams processed successfully. Nexus throughput optimal.")
