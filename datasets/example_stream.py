from dataproc.job import Job
from dataproc.stream import Stream

dataset_paths = [
    ('stream1', 'data/dataset1'),
    ('stream2', 'data/dataset2')
]

def main():
    print('in example stream main')

    job = Job('example', test=True)
    job.init_spark()

    for ds in dataset_paths:
        print(f'setting up {ds[0]}')
        stream = Stream(ds[0], job)
        df = stream.input(ds[1], lambda data_frame_reader: data_frame_reader.format('csv'))

        # random transforms here on df
        print(f'doing transform stuff for {ds[0]}')

        stream.output(df, f'/path/to/output/{ds[0]}', lambda data_frame_writer: data_frame_writer.format('noop').mode('overwrite'))

    job.start()


if __name__ == '__main__':
    main()