from typing import Callable, List
from dataproc.application import Application
from pyspark.sql import SparkSession


class Job:
    def __init__(self, name, test=False):
        print("in Job __init__")
        self.app = None
        self.name = name
        self.stream_writers: List[Callable] = []
        self.test = test
        self.spark = None

    def __log(self, msg):
        print(f'[Log] {msg}')

    def __repr__(self):
        print(f'Job {self.name}')

    def _init_app(self):
        self.app = Application()

    def init_spark(self):
        self.spark = SparkSession.builder.master('local').getOrCreate()

    def start(self):
        if self.app == None:
            self._init_app()

        self.__log("Starting job")

        if not self.test:
            self.__log(f"Executing {len(self.stream_writers)} streams")
            for sw in self.stream_writers:
                sw()

    def read(self):
        self.__log("getting read option from spark")
        return {}

    def write(self, df):
        self.__log("returning writer class")
        return {}

    def register_stream_writer(self, writer: Callable):
        self.stream_writers.append(writer)