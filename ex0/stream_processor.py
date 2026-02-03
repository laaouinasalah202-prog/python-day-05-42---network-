from typing import Any
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        return True

    def format_output(self, result: str) -> str:
        return result


class NumericProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        try:
            for i in data:
                int(i)
            return True
        except ValueError:
            return False

    def process(self, data: list[int]) -> str:
        if self.validate(data):
            return f" {len(data)} {sum(data)} {sum(data)/len(data)}"
        else:
            print("Processing data:  invalid input")
            return "Numeric data not verified because of Incorrect data type"

    def format_output(self, result: str) -> str:
        res = result.split()
        return f"Processed {res[0]} numeric values, sum={res[1]}, avg={res[2]}"


class TextProcessor(DataProcessor):

    def validate(self, data: str) -> bool:
        try:
            if not isinstance(data, str):
                raise ValueError()
            return True
        except ValueError:
            return False

    def process(self, data: Any) -> str:
        if self.validate(data):
            return f"{len(data)} {len(data.split())}"
        else:
            return "Processing data: ERROR: data is not string"

    def format_output(self, result: str) -> str:
        r = result.split()

        return f"Processed text: {r[0]} characters, {r[1]} words"


class LogProcessor(DataProcessor):

    def validate(self, data: str) -> bool:
        valide = ("ERROR", "WARNING", "ALERT", "INFO", "FATAL")
        if not isinstance(data, str):
            return False
        if any(data.startswith(i) for i in valide):
            return True
        else:
            return False

    def process(self, data: Any) -> str:
        if data.startswith("ERROR"):
            return "[ALERT] ERROR level detected: Connection timeout"
        if data.startswith("INFO"):
            return "[INFO] INFO level detected: System ready"

    def format_output(self, result: str) -> str:
        return f"{result}"


print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")
print("Initializing Numeric Processor...")
data = [1, 2, 3, 4, 5]
num = NumericProcessor()
i = num.process(data)
print(f"Processing data: {data}")
if num.validate(data):
    print("Validation: Numeric data verified")
else:
    print("Validation : data not verified")
print(num.format_output(i))

print("\nInitializing Text Processor...")

text = TextProcessor()
data1 = "Hello Nexus World"
print(f"Processing data: {data1}")
j = text.process(data1)
if text.validate(data1):
    print("Validation: Text data verified")
    print(f"Processed: {text.format_output(j)}")
else:
    print("Validation : data not verified")
    print(text.format_output(j))

print("\nInitializing Log Processor...")

log = LogProcessor()
data2 = "ERROR: Connection timeout"
print(f"Processing data: {data2}")
if log.validate("ERROR"):
    print("Validation: Log entry verified")
    print("Output: [ALERT] ERROR level detected: Connection timeout")
else:
    print("validation: ")

print("\n=== Polymorphic Processing Demo ===")
print("Processing multiple data types through same interface...")

result1 = NumericProcessor()
d1 = [2, 2, 2]
r1 = result1.process(d1)
print(f"Result 1: {result1.format_output(r1)}")
result2 = TextProcessor()
d2 = "Soft thunder"
r2 = result2.process(d2)
print(f"Result 2: {result2.format_output(r2)}")

result3 = LogProcessor()
d3 = "INFO : system ready"
r3 = result3.process(d3)
print(f"Result 3: {result3.format_output(r3)}")
print("\nFoundation systems online. Nexus ready for advanced streams.")
