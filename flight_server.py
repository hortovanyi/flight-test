import pathlib

import pyarrow as pa
import pyarrow.flight
import pyarrow.parquet
import pyarrow.dataset as ds


class FlightServer(pa.flight.FlightServerBase):

    def __init__(self, location="grpc://0.0.0.0:8815",
                repo=pathlib.Path("/u02/data/poc5/test"), **kwargs):
        super(FlightServer, self).__init__(location, **kwargs)
        self._location = location
        self._repo = repo

    def _make_flight_info(self, dataset):
        dataset_path = self._repo / dataset
        schema = pa.parquet.read_schema(dataset_path)
        metadata = pa.parquet.read_metadata(dataset_path)
        descriptor = pa.flight.FlightDescriptor.for_path(
            dataset.encode('utf-8')
        )
        endpoints = [pa.flight.FlightEndpoint(dataset, [self._location])]
        return pyarrow.flight.FlightInfo(schema,
                                        descriptor,
                                        endpoints,
                                        metadata.num_rows,
                                        metadata.serialized_size)


    def _make_flight_info_dataset(self, criteria):
        ds_name=criteria.decode('utf-8')

        dataset = self._make_dataset(criteria)

        schema = dataset.schema
        descriptor = pa.flight.FlightDescriptor.for_path(criteria)
        endpoints = [pa.flight.FlightEndpoint(ds_name, [self._location])]

        return pyarrow.flight.FlightInfo(schema,
                                        descriptor,
                                        endpoints,
                                        dataset.count_rows(),
                                        -1)

    def _make_dataset(self, criteria):
        ds_name=criteria.decode('utf-8')
        name, name_suffix=ds_name.split('.')

        ds_paths=list(self._repo.glob('**/'+name+'_*.parquet'))
        ds_paths.sort()

        dataset = ds.dataset(ds_paths, format='parquet')

        return dataset

    def list_flights(self, context, criteria):
        print("list_flights - criteria: '"+ criteria.decode('utf-8')+"'")
        if criteria == b'':
            for dataset in self._repo.glob('**/*.parquet'):
                yield self._make_flight_info(dataset.name)

        elif criteria.decode('utf-8').endswith('.dataset'):
            yield self._make_flight_info_dataset(criteria)
        else:
            for dataset in self._repo.glob(criteria.decode('utf-8')):
                yield self._make_flight_info(dataset.name)

    def get_flight_info(self, context, descriptor):
        if descriptor.path[0].decode('utf-8').endswith('.dataset'):
            fi = self._make_flight_info_dataset(descriptor.path[0])
        else:
            fi = self._make_flight_info(descriptor.path[0].decode('utf-8'))

        return fi

    def do_put(self, context, descriptor, reader, writer):
        dataset = descriptor.path[0].decode('utf-8')
        dataset_path = self._repo / dataset
        # Read the uploaded data and write to Parquet incrementally
        with dataset_path.open("wb") as sink:
            with pa.parquet.ParquetWriter(sink, reader.schema) as writer:
                for chunk in reader:
                    writer.write_table(pa.Table.from_batches([chunk.data]))

    def do_get(self, context, ticket):
        if ticket.ticket.decode('utf-8').endswith('.dataset'):
            gen = self._do_get_dataset(context,ticket)
        else:
            gen = self._do_get_parquet(context,ticket)

        return gen

    def _do_get_dataset(self, context, ticket):
        dataset = self._make_dataset(ticket.ticket)

        return pa.flight.GeneratorStream(
            dataset.schema, dataset.to_batches())

    def _do_get_parquet(self, context, ticket):
        dataset = ticket.ticket.decode('utf-8')
        # Stream data from a file
        dataset_path = self._repo / dataset
        reader = pa.parquet.ParquetFile(dataset_path)
        return pa.flight.GeneratorStream(
            reader.schema_arrow, reader.iter_batches())

    def list_actions(self, context):
        return [
            ("drop_dataset", "Delete a dataset."),
        ]

    def do_action(self, context, action):
        if action.type == "drop_dataset":
            self.do_drop_dataset(action.body.to_pybytes().decode('utf-8'))
        else:
            raise NotImplementedError

    def do_drop_dataset(self, dataset):
        dataset_path = self._repo / dataset
        dataset_path.unlink()


if __name__ == '__main__':
    server = FlightServer()
    server._repo.mkdir(exist_ok=True)
    server.serve()
