import argparse
import json
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions


# Filter stage: keep a record only if every measurement is present.
# A dropped reading arrives as JSON null, which json.loads turns into None.
def isComplete(record):
    return all(value is not None for value in record.values())


# Convert stage: kPa to psi and Celsius to Fahrenheit. Humidity is unchanged.
def convertUnits(record):
    record = dict(record)
    record['pressure'] = record['pressure'] / 6.895
    record['temperature'] = record['temperature'] * 1.8 + 32
    return record


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest='input', required=True,
                        help='Pub/Sub subscription to read the readings from.')
    parser.add_argument('--output', dest='output', required=True,
                        help='Pub/Sub topic to send the converted readings to.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        readings = (p | 'Read from PubSub' >> beam.io.ReadFromPubSub(subscription=known_args.input)
                      | 'toDict' >> beam.Map(lambda x: json.loads(x.decode('utf-8'))))

        converted = (readings | 'Filter' >> beam.Filter(isComplete)
                              | 'Convert' >> beam.Map(convertUnits))

        (converted | 'toBytes' >> beam.Map(lambda x: json.dumps(x).encode('utf-8'))
                   | 'Write to PubSub' >> beam.io.WriteToPubSub(topic=known_args.output))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
