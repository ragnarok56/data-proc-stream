# from dataproc.job import Job
from typing import Callable
from pyspark.sql import DataFrame
from pyspark.sql import DataFrameReader, DataFrameWriter

from dataproc.job import Job

class Stream:
    def __init__(self, name, job: Job):
        self.name = name
        self.input_paths = []
        self.output_path = None
        self.job = job

    def input(self, path: str, func: Callable[[DataFrameReader], DataFrameReader]):
        self.input_paths = [path]

        # do some batch/stream detection somewhere
        reader = func(self.job.spark.read)

        return reader.load(self.input_paths)

    def output(self, df: DataFrame, path: str, func: Callable[[DataFrameWriter], DataFrameWriter]):
        self.output_path = path

        # do some batch/stream detection somewhere
        writer = func(df.write)

        def stream_writer():
            # this actually does the write
            writer.save(self.output_path)

        self.job.register_stream_writer(stream_writer)













# df = spark.read.format('json').options('asdkfjasdf', 'asdjfkasdjf').load('/some/path')