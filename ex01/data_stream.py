from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional

class DataStream(ABC):
    def __init__(self, ID):
        self.ID = ID
        self.err = 0

    @classmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return [
            {
                "stream_id" : self.ID,
                "type" : self.type_dat,
                "Error_count": self.err
            }
        ]

class SensorStream(DataStream):
    def __init__(self, ID: str):
        super().__init__(ID)
        self.type_data = "Environmental Data"
        self.temp_readings = []
        self.humidity_readings = []
        self.pressure_readings = []
        self.avg = 0
        self.total_processed = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        print(f"Stream ID: {self.ID}, Type: {self.type_data}")
        try:
            for data in data_batch:
                data_str = str(data)
                print(f"Processing sensor batch:{data_str}")
                if "temp:" in data_str:
                    temp = float(data_str.split("temp:")[1].split(",")[0])
                    self.temp_readings.append(temp)
                if "humidity:" in data_str:
                    humidity = float(data_str.split("humidity:")[1].split(",")[0])
                    self.humidity_readings.append(humidity)
                if "pressure:" in data_str:
                    pressure = float(data_str.split("pressure:")[1].split("]")[0])
                    self.pressure_readings.append(pressure)
            self.total_processed += 1
            self.avg = sum(self.temp_readings)/ self.total_processed
            return f"Sensor analysis: {self.total_processed} readings processed, avg temp: {self.avg}°C"
        except Exception as e:
            print(e)
            self.err += 1

class TransactionStream(DataStream):
    def __init__(self,ID: str):
        super().__init__(ID)
        self.type_data = "Financial Data"
        self.buy_readings = []
        self.sell_readings = []
        self.net_flow = 0
        self.total_processed = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        print(f"Stream ID: {self.ID}, Type: {self.type_data}")
        
        for d in data_batch:
            d = str(d)
            if "buy" in d:
                b = int(d.split("buy:")[1])
                self.buy_readings.append(b)
            if "sell" in d:
                b = int(d.split("sell:")[1])
                self.sell_readings.append(b)
        print(self.buy_readings)
class EventStream(DataStream):
    ...

class StreamProcessor():
    ...



print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")
print("Initializing Sensor Stream...")

data1 = ["temp:22.5, humidity:65, pressure:1013"]
Sensor = SensorStream("SENSOR_001")
result1 = Sensor.process_batch(data1)
print(result1)

print("\nInitializing Transaction Stream...")
data2 = ["buy:100", "sell:150", "buy:75"]
finance = TransactionStream("TRANS_001")
result2 = finance.process_batch(data2)
print(result2)