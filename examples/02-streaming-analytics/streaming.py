"""
Real-time Streaming Analytics with Apache Beam
Demonstrates windowing, triggers, and state management.
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows, SlidingWindows
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode
import json


class ParseEventFn(beam.DoFn):
    """Parse JSON events from stream."""

    def process(self, element):
        try:
            event = json.loads(element)
            yield event
        except json.JSONDecodeError:
            # Log error or write to dead letter queue
            pass


class CalculateMetrics(beam.CombineFn):
    """Calculate streaming metrics (count, sum, avg)."""

    def create_accumulator(self):
        return (0, 0.0)  # (count, sum)

    def add_input(self, accumulator, input):
        count, total = accumulator
        return count + 1, total + input

    def merge_accumulators(self, accumulators):
        counts, totals = zip(*accumulators)
        return sum(counts), sum(totals)

    def extract_output(self, accumulator):
        count, total = accumulator
        return {
            'count': count,
            'sum': total,
            'avg': total / count if count > 0 else 0
        }


def run_streaming_pipeline():
    """Run real-time analytics pipeline."""

    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        # Read from Pub/Sub or Kafka
        events = (p
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(
                subscription='projects/PROJECT/subscriptions/SUBSCRIPTION'
            )
            | 'ParseEvents' >> beam.ParDo(ParseEventFn())
        )

        # Fixed windows (1 minute)
        fixed_windowed = (events
            | 'FixedWindow' >> beam.WindowInto(
                FixedWindows(60),  # 1 minute windows
                trigger=AfterWatermark(
                    early=AfterProcessingTime(10)  # Early firing every 10 seconds
                ),
                accumulation_mode=AccumulationMode.ACCUMULATING
            )
            | 'ExtractValue' >> beam.Map(lambda x: (x['user_id'], x['value']))
            | 'ComputeMetrics' >> beam.CombinePerKey(CalculateMetrics())
        )

        # Sliding windows (5 min window, 1 min slide)
        sliding_windowed = (events
            | 'SlidingWindow' >> beam.WindowInto(
                SlidingWindows(300, 60)  # 5 min window, 1 min slide
            )
            | 'CountPerUser' >> beam.combiners.Count.PerElement()
        )

        # Write results
        (fixed_windowed
            | 'FormatFixed' >> beam.Map(lambda x: json.dumps({
                'user_id': x[0],
                'metrics': x[1],
                'window': 'fixed'
            }))
            | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                'project:dataset.fixed_metrics',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )


if __name__ == '__main__':
    run_streaming_pipeline()
