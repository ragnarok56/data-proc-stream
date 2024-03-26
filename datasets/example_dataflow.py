from dataproc.dataflow import R, W, DataFlow
from dataproc.job import Job
from pyspark.sql import DataFrame

dataset_paths = [
    ('stream1', 'data/dataset1'),
    ('stream2', 'data/dataset2')
]

class ExampleDataFlow(DataFlow[R, W]):
    def read(self, reader: R):
        print("defining reader")
        return reader.format('csv')

    def write(self, writer: W):
        print("defining writer")
        return writer.format('noop').mode('append')

    def transform(self, df: DataFrame):
        print("doing transform stuff")
        return df


def main():
    print('in example dataflow main')

    job = Job('example', test=False, stream=False)
    job.init_spark()

    flows = []

    for ds in dataset_paths:
        print(f'setting up {ds[0]}')

        flow = ExampleDataFlow(ds[1])

        job.register_data_flow(flow)

    job.start()

if __name__ == '__main__':
    main()