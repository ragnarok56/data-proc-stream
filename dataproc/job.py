from typing import Callable, List, Union
from dataproc.application import Application
from pyspark.sql import SparkSession

from dataproc.dataflow import DataFlow, DataFlowBatch, DataFlowStream
from dataproc.listener.streaming import StreamingListener

class Job:
    def __init__(self, name, test=False, stream=True):
        print("in Job __init__")
        self.app = None
        self.name = name
        self.stream_writers: List[Callable] = []
        self.flows: Union[List[DataFlowBatch], List[DataFlowStream]] = []
        self.test = test
        self.stream = stream
        self.spark: SparkSession = None

    def __log(self, msg):
        print(f'[Log] {msg}')

    def __repr__(self):
        print(f'Job {self.name}')

    def _init_app(self):
        self.app = Application()

    def init_spark(self):
        path = '/Users/kevinnacios/Projects/github/ragnarok56/data-proc-stream/scala/dataproc/target/scala-2.13/dataproc_2.13-0.1.0-SNAPSHOT.jar'
        spark: SparkSession = (SparkSession
            .builder
            .master('local')
            .config('spark.sql.streaming.schemaInference', 'true')
            # .config('spark.jars.packages', 'net.nacios.spark:dataproc_2.13:0.1.0-SNAPSHOT')
            .config('spark.jars', path)
            .config('spark.driver.extraClassPath', path)
            .config('spark.executor.extraClassPath', path)
            .config('spark.sql.queryExecutionListeners', 'net.nacios.spark.listener.CustomQueryExecutionListener')
            .getOrCreate()
        )
        print(spark.sparkContext._jsc.sc().listJars())
        spark.streams.addListener(StreamingListener())

        self.spark = spark

    def start(self):
        if self.app == None:
            self._init_app()

        self.__log("Starting job")

        if not self.test:
            if self.stream_writers:
                self.__log(f"Executing {len(self.stream_writers)} streams")
                for sw in self.stream_writers:
                    sw()

            if self.flows:
                self.__log(f"Executing {len(self.flows)} data flows")
                for flow in self.flows:
                    if self.stream:
                        self.execute_data_flow_stream(flow)
                    else:
                        self.execute_data_flow_batch(flow)

        if self.stream:
            self.spark.streams.awaitAnyTermination()


    def read(self):
        self.__log("getting read option from spark")
        return {}

    def write(self, df):
        self.__log("returning writer class")
        return {}

    def register_stream_writer(self, writer: Callable):
        self.stream_writers.append(writer)

    def register_data_flow(self, flow: DataFlow):
        self.flows.append(flow)

    def execute_data_flow_batch(self, flow: DataFlowBatch):
        reader = flow.read(self.spark.read)

        df = reader.load(flow.input_path)

        df = flow.transform(df)

        writer = flow.write(df.write)

        self.__log("writing batch")
        writer.save()

    def execute_data_flow_stream(self, flow: DataFlowStream):
        reader = flow.read(self.spark.readStream)

        df = reader.load(flow.input_path)

        df = flow.transform(df)

        writer = flow.write(df.writeStream)

        self.__log("starting stream")
        return writer.start()