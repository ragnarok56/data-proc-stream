from dataproc.job import Job

dataset_paths = [
    ('stream1', '/path/to/stream1'),
    ('stream2', '/path/to/stream2')
]

def main():
    print('in example main')

    job = Job('example')
    job.init_spark()

    for ds in dataset_paths:
        print(f'setting up {ds[0]}')
        df = job.read() # plus other stuff

        # random transforms here on df
        print(f'doing transform stuff for {ds[0]}')

        job.write(df) # plus other stuff

    job.start()


if __name__ == '__main__':
    main()