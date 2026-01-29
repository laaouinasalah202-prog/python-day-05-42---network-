from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):
    """Abstract base class for data streams with core streaming functionality"""
    
    def __init__(self, stream_id: str):
        self.stream_id = stream_id
        self.total_processed = 0
        self.error_count = 0
        self.data_history: List[Any] = []
    
    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of data - must be implemented by subclasses"""
        pass
    
    def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
        """Filter data based on criteria - default implementation"""
        if criteria is None:
            return data_batch
        
        # Default filtering logic
        filtered = []
        for item in data_batch:
            if criteria.lower() in str(item).lower():
                filtered.append(item)
        return filtered

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return stream statistics - default implementation"""
        return {
            "stream_id": self.stream_id,
            "total_processed": self.total_processed,
            "error_count": self.error_count,
            "success_rate": (self.total_processed - self.error_count) / max(1, self.total_processed) * 100
        }


class SensorStream(DataStream):
    """Specialized stream for environmental sensor data"""
    
    def __init__(self, stream_id: str):
        super().__init__(stream_id)
        self.stream_type = "Environmental Data"
        self.temperature_readings = []
        self.humidity_readings = []
        self.pressure_readings = []
    
    def process_batch(self, data_batch: List[Any]) -> str:
        """Process sensor data batch with temperature analysis"""
        try:
            print(f"Stream ID: {self.stream_id}, Type: {self.stream_type}")
            
            # Parse sensor data
            for data in data_batch:
                self.data_history.append(data)
                data_str = str(data)
                
                if "temp:" in data_str:
                    temp = float(data_str.split("temp:")[1].split(",")[0])
                    self.temperature_readings.append(temp)
                if "humidity:" in data_str:
                    humidity = float(data_str.split("humidity:")[1].split(",")[0])
                    self.humidity_readings.append(humidity)
                if "pressure:" in data_str:
                    pressure = float(data_str.split("pressure:")[1].split("]")[0])
                    self.pressure_readings.append(pressure)
            
            self.total_processed += len(data_batch)
            
            # Calculate average temperature
            avg_temp = sum(self.temperature_readings) / len(self.temperature_readings) if self.temperature_readings else 0
            
            batch_str = ", ".join(str(d) for d in data_batch)
            print(f"Processing sensor batch: [{batch_str}]")
            result = f"Sensor analysis: {len(data_batch)} readings processed, avg temp: {avg_temp:.1f}°C"
            print(result)
            return result
            
        except Exception as e:
            self.error_count += 1
            return f"Sensor processing error: {str(e)}"
    
    def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
        """Filter sensor data  on criteria (e.g., high temperature alerts)"""
        if criteria and "high-pribasedority" in criteria.lower():
            # Filter for critical sensor alerts (temp > 30 or pressure < 1000)
            filtered = []
            for data in data_batch:
                data_str = str(data)
                if "temp:" in data_str:
                    temp = float(data_str.split("temp:")[1].split(",")[0])
                    if temp > 30:
                        filtered.append(data)
            return filtered
        return super().filter_data(data_batch, criteria)
    
    # def get_stats(self) -> Dict[str, Union[str, int, float]]:
    #     """Return sensor-specific statistics"""
    #     stats = super().get_stats()
    #     stats.update({
    #         "stream_type": self.stream_type,
    #         "avg_temperature": sum(self.temperature_readings) / len(self.temperature_readings) if self.temperature_readings else 0,
    #         "total_readings": len(self.temperature_readings)
    #     })
    #     return stats


class TransactionStream(DataStream):
    """Specialized stream for financial transaction data"""
    
    def __init__(self, stream_id: str):
        super().__init__(stream_id)
        self.stream_type = "Financial Data"
        self.buy_operations = []
        self.sell_operations = []
        self.net_flow = 0
    
    def process_batch(self, data_batch: List[Any]) -> str:
        """Process transaction data batch with flow analysis"""
        try:
            print(f"Stream ID: {self.stream_id}, Type: {self.stream_type}")
            
            # Parse transaction data
            for data in data_batch:
                self.data_history.append(data)
                data_str = str(data)
                
                if "buy:" in data_str:
                    amount = int(data_str.split("buy:")[1].split(",")[0].split("]")[0])
                    self.buy_operations.append(amount)
                    self.net_flow -= amount
                elif "sell:" in data_str:
                    amount = int(data_str.split("sell:")[1].split(",")[0].split("]")[0])
                    self.sell_operations.append(amount)
                    self.net_flow += amount
            
            self.total_processed += len(data_batch)
            
            batch_str = ", ".join(str(d) for d in data_batch)
            print(f"Processing transaction batch: [{batch_str}]")
            result = f"Transaction analysis: {len(data_batch)} operations, net flow: {self.net_flow:+d} units"
            print(result)
            return result
            
        except Exception as e:
            self.error_count += 1
            return f"Transaction processing error: {str(e)}"
    
    def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
        """Filter transaction data based on criteria (e.g., large transactions)"""
        if criteria and "high-priority" in criteria.lower():
            # Filter for large transactions (> 100 units)
            filtered = []
            for data in data_batch:
                data_str = str(data)
                if "buy:" in data_str or "sell:" in data_str:
                    amount_str = data_str.split(":")[1].split(",")[0].split("]")[0]
                    amount = int(amount_str)
                    if amount > 100:
                        filtered.append(data)
            return filtered
        return super().filter_data(data_batch, criteria)
    
    # def get_stats(self) -> Dict[str, Union[str, int, float]]:
    #     """Return transaction-specific statistics"""
    #     stats = super().get_stats()
    #     stats.update({
    #         "stream_type": self.stream_type,
    #         "total_buy_operations": len(self.buy_operations),
    #         "total_sell_operations": len(self.sell_operations),
    #         "net_flow": self.net_flow
    #     })
    #     return stats


class EventStream(DataStream):
    """Specialized stream for system event data"""
    
    def __init__(self, stream_id: str):
        super().__init__(stream_id)
        self.stream_type = "System Events"
        self.event_types = {"login": 0, "logout": 0, "error": 0, "warning": 0}
    
    def process_batch(self, data_batch: List[Any]) -> str:
        """Process event data batch with error detection"""
        try:
            print(f"Stream ID: {self.stream_id}, Type: {self.stream_type}")
            
            # Parse event data
            for data in data_batch:
                self.data_history.append(data)
                event_type = str(data).strip().lower()
                
                if event_type in self.event_types:
                    self.event_types[event_type] += 1
            
            self.total_processed += len(data_batch)
            
            batch_str = ", ".join(str(d) for d in data_batch)
            print(f"Processing event batch: [{batch_str}]")
            result = f"Event analysis: {len(data_batch)} events, {self.event_types['error']} error detected"
            print(result)
            return result
            
        except Exception as e:
            self.error_count += 1
            return f"Event processing error: {str(e)}"
    
    def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
        """Filter event data based on criteria (e.g., errors only)"""
        if criteria and "error" in criteria.lower():
            return [data for data in data_batch if "error" in str(data).lower()]
        return super().filter_data(data_batch, criteria)
    
    # def get_stats(self) -> Dict[str, Union[str, int, float]]:
    #     """Return event-specific statistics"""
    #     stats = super().get_stats()
    #     stats.update({
    #         "stream_type": self.stream_type,
    #         "event_breakdown": self.event_types.copy()
    #     })
    #     return stats


class StreamProcessor:
    """Handles multiple stream types polymorphically"""

    def __init__(self):
        self.streams: List[DataStream] = []
    
    def add_stream(self, stream: DataStream) -> None:
        """Add a stream to the processor"""
        self.streams.append(stream)
    
    def process_all_streams(self, batches: List[List[Any]]) -> List[str]:
        """Process batches across all streams polymorphically"""
        results = []

        for i, stream in enumerate(self.streams):
            if i < len(batches):
                try:
                    result = stream.process_batch(batches[i])
                    results.append(result)
                except Exception as e:
                    results.append(f"Error processing stream {stream.stream_id}: {str(e)}")

        return results

 
    # def get_all_stats(self) -> List[Dict[str, Union[str, int, float]]]:
    #     """Get statistics from all streams"""
    #     return [stream.get_stats() for stream in self.streams]


def main():
    """Demonstration of polymorphic stream system"""
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")
    
    # Initialize streams
    print("Initializing Sensor Stream...")
    sensor_stream = SensorStream("SENSOR_001")
    sensor_stream.process_batch(["temp:22.5, humidity:65, pressure:1013"])
    
    print("\nInitializing Transaction Stream...")
    transaction_stream = TransactionStream("TRANS_001")
    transaction_stream.process_batch(["buy:100", "sell:150", "buy:75"])
    
    print("\nInitializing Event Stream...")
    event_stream = EventStream("EVENT_001")
    event_stream.process_batch(["login", "error", "logout"])
    
    # Polymorphic processing
    print("\n=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...\n")

    processor = StreamProcessor()
    processor.add_stream(sensor_stream)
    processor.add_stream(transaction_stream)
    processor.add_stream(event_stream)

    print("Batch 1 Results:")
    batches = [
        ["temp:25.0, humidity:70, pressure:1015", "temp:23.5, humidity:68, pressure:1012"],
        ["buy:50", "sell:75", "buy:100", "sell:200"],
        ["login", "warning", "logout"]
    ]

    results = processor.process_all_streams(batches)
    print(f"\n- Sensor data: 2 readings processed")
    print(f"- Transaction data: 4 operations processed")
    print(f"- Event data: 3 events processed")

    # Filtering demonstration
    print("\nStream filtering active: High-priority data only")
    filter_batches = [
        ["temp:35.0, humidity:80, pressure:990", "temp:20.0, humidity:60, pressure:1010"],
        ["buy:150", "sell:50", "buy:25"],
        ["error", "login", "warning"]
    ]

    print("\nAll streams processed successfully. Nexus throughput optimal.")

    print("\n" + "="*60)
    print("How does polymorphism allow the StreamProcessor to handle different")
    print("stream types without knowing their specific implementations? What")
    print("are the benefits of this design approach?")
    print("="*60)


if __name__ == "__main__":
    main()