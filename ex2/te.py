from typing import Protocol

class Flyable(Protocol):
    def fly(self) -> None:
        ...

class Bird:
    def fly(self) -> None:
        print("Flying!")

class Airplane:
    def fly(self) -> None:
        print("Jet noises")

        
def make_it_fly(thing: Flyable):
    thing.fly()

make_it_fly(Bird())
make_it_fly(Airplane())