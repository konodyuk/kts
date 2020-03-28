from typing import Optional, Tuple


SPLITTER = '__KTS__'


class RunID:
    def __init__(self, function_name: str, fold: str, input_frame: Optional[str] = None):
        self.function_name = function_name
        self.fold = fold
        self.input_frame = input_frame

    @staticmethod
    def from_column_name(column_name: str) -> 'RunID':
        args = column_name.split(SPLITTER)[1:]
        return RunID(*args)

    @staticmethod
    def from_alias_name(alias_name: str) -> 'RunID':
        args = alias_name.split(SPLITTER)
        return RunID(*args)

    @staticmethod
    def from_state_name(state_name: str) -> 'RunID':
        args = state_name.split(SPLITTER)
        return RunID(*args)

    def get_column_name(self, column_name: str) -> str:
        return f"{column_name}{SPLITTER}{self.function_name}{SPLITTER}{self.fold}"

    def get_alias_name(self) -> str:
        return f"{self.function_name}{SPLITTER}{self.fold}{SPLITTER}{self.input_frame}"

    def get_state_name(self) -> str:
        return f"{self.function_name}{SPLITTER}{self.fold}"

    def get_state_key(self) -> Tuple[str, str]:
        return self.function_name, self.fold

    @property
    def state_id(self) -> Tuple[str, str]:
        return self.get_state_key()

    def __eq__(self, other):
        return (self.function_name == other.function_name
                and self.fold == other.fold
                and self.input_frame == other.input_frame)

    def __hash__(self):
        return hash(self.get_alias_name())

    def __str__(self):
        return f"RunID({repr(self.function_name)}, {repr(self.fold)}, {repr(self.input_frame)})"

    def __repr__(self):
        return self.__str__()

    @property
    def registration_name(self):
        # Splits by __ in case of generic (name__param1_param2)
        # Otherwise does nothing
        return self.function_name.partition('__')[0]
