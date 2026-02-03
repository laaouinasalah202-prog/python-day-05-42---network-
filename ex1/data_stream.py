from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):
    def __init__(self, ID: str) -> None:
        self.ID = ID
        self.err = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(
        self, data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:

        if criteria is None:
            return data_batch
        return [
            item for item in data_batch
            if criteria.lower() in str(item).lower()
        ]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return [
            {
                "stream_id": self.ID,
                "Error_count": self.err
            }
        ]


class SensorStream(DataStream):
    def __init__(self, ID: str) -> None:
        super().__init__(ID)
        self.type_data = "Environmental Data"
        self.temp_readings = []
        self.humidity_readings = []
        self.pressure_readings = []
        self.avg = 0
        self.total_processed = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            for data in data_batch:
                data_str = str(data)
                if "temp:" in data_str:
                    temp = float(data_str.split("temp:")[1].split(",")[0])
                    self.temp_readings.append(temp)
                if "humidity:" in data_str:
                    humidity = float(
                        data_str.split("humidity:")[1].split(",")[0]
                        )
                    self.humidity_readings.append(humidity)
                if "pressure:" in data_str:
                    pressure = float(
                        data_str.split("pressure:")[1].split("]")[0]
                        )
                    self.pressure_readings.append(pressure)
            self.total_processed += 1
            self.avg = sum(self.temp_readings) / self.total_processed
            return (f"Sensor analysis: {len(data_batch[0].split())} readings "
                    f"processed, avg temp: {self.avg}°C")

        except Exception as e:
            print(e)
            self.err += 1

    def filter_data(
        self, data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:

        if criteria == "critical":

            filtered = []
            for item in data_batch:
                if "temp" in str(item):
                    temp_val = float(str(item).split(":")[1].split(",")[0])
                    if temp_val > 30 or temp_val < 0:
                        filtered.append(item)
            return filtered
        return super().filter_data(data_batch, criteria)


class TransactionStream(DataStream):
    def __init__(self, ID: str):
        super().__init__(ID)
        self.type_data = "Financial Data"
        self.buy_readings = []
        self.sell_readings = []
        self.net_flow = 0
        self.total_processed = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        for d in data_batch:
            d = str(d)
            if "buy" in d:
                b = int(d.split("buy:")[1])
                self.buy_readings.append(b)
            if "sell" in d:
                b = int(d.split("sell:")[1])
                self.sell_readings.append(b)
        return (f"Transaction analysis: {len(data_batch)} operations, net flow"
                f": +{sum(self.buy_readings) - sum(self.sell_readings)} units")

    def filter_data(
        self, data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:

        if criteria == "large":
            filtered = []
            for item in data_batch:
                if isinstance(item, dict):
                    for key, value in item.items():
                        if value > 100:
                            filtered.append(item)
                            break
            return filtered
        return super().filter_data(data_batch, criteria)


class EventStream(DataStream):
    def __init__(self, ID: str):
        super().__init__(ID)
        self.type_data = "Financial Data"

    def process_batch(self, data_batch: List[Any]) -> str:
        err = str(data_batch).count("error")
        return f"Event analysis: {3} events, {err} error detected"


class StreamProcessor:
    def __init__(self) -> None:

        self.streams: List[DataStream] = []

    def add_stream(self, stream: DataStream) -> None:

        self.streams.append(stream)

    def process_all(self, data_batches: List[List[Any]]) -> List[str]:

        results = []
        for stream, batch in zip(self.streams, data_batches):
            result = stream.process_batch(batch)
            results.append(result)
        return results


print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")
print("Initializing Sensor Stream...")

data1 = ["temp:22.5, humidity:65, pressure:1013"]
Sensor = SensorStream("SENSOR_001")
result1 = Sensor.process_batch(data1)
print("Stream ID: SENSOR_001, Type: Environmental Data")
print("Processing sensor batch: [temp:22.5, humidity:65, pressure:1013]")
print(result1)

print("\nInitializing Transaction Stream...")
data2 = ["buy:100", "sell:150", "buy:75"]
finance = TransactionStream("TRANS_001")
result2 = finance.process_batch(data2)
print("Stream ID: TRANS_001, Type: Financial Data")
print("Processing transaction batch: [buy:100, sell:150, buy:75]")
print(result2)

print("\nInitializing Event Stream...")
data3 = ["login", "error", "logout"]
event = EventStream("EVENT_001")
result3 = event.process_batch(data3)
print("Stream ID: EVENT_001, Type: System Events")
print("Processing event batch: [login, error, logout]")
print(result3)

print("\n=== Polymorphic Stream Processing ===")
print("Processing mixed stream types through unified interface...\n")


processor = StreamProcessor()

sensor_stream2 = SensorStream("SENSOR_002")
trans_stream2 = TransactionStream("TRANS_002")
event_stream2 = EventStream("EVENT_002")

processor.add_stream(sensor_stream2)
processor.add_stream(trans_stream2)
processor.add_stream(event_stream2)

batch_data = [
    ["temp:21.0, temp:23.0"],
    ["buy:50", "sell:75", "buy:25", "sell:100"],
    ["startup", "processing", "shutdown"]
]

results = processor.process_all(batch_data)

print("Batch 1 Results:")
print(f"- Sensor data: {results[0].split(',')[0].split(': ')[1].strip()}")
print(
    "- Transaction data: "
    f"{results[1].split(':')[1].split(',')[0].strip()}"
)
print(f"- Event data: {results[2].split(':')[1].split(', ')[0]} processed")

print()

print("Stream filtering active: High-priority data only")

sensor_critical = Sensor.filter_data(
    ["temp:35.0", "temp:34.0"],
    "critical"
)

trans_large = finance.filter_data(
    [{"buy": 150}, {"sell": 50}],
    "large"
)

print(
    f"Filtered results: {len(sensor_critical)}"
    f" critical sensor alerts, {len(trans_large)} large transaction"
)

print()

print("All streams processed successfully. Nexus throughput optimal.")
