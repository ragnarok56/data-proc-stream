from abc import ABC, abstractmethod
from pyspark.sql import DataFrameReader, DataFrameWriter, DataFrame
from pyspark.sql.streaming.readwriter import DataStreamReader, DataStreamWriter
from typing import Generic, TypeVar, Union

R = TypeVar('R', DataFrameReader, DataStreamReader)
W = TypeVar('W', DataFrameWriter, DataStreamWriter)


class DataFlow(ABC, Generic[R, W]):
    def __init__(self, input_path: Union[str, list[str]]):
        self.input_path = input_path

    @abstractmethod
    def read(self, reader: R) -> R:
        pass

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        pass

    @abstractmethod
    def write(self, writer: W) -> W:
        pass


class DataFlowBatch(DataFlow[DataFrameReader, DataFrameWriter]):
    pass

class DataFlowStream(DataFlow[DataStreamReader, DataStreamWriter]):
    pass


