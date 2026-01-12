"""
Apache Beam WordCount Example
A classic data processing pipeline demonstrating core Beam concepts.
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText, WriteToText
import argparse
import re


class ExtractWordsFn(beam.DoFn):
    """Extract words from text."""

    def process(self, element):
        """Split text into words.

        Args:
            element: Line of text

        Yields:
            Individual words (lowercase)
        """
        # Remove punctuation and split into words
        words = re.findall(r'[A-Za-z\']+', element)
        for word in words:
            if word:
                yield word.lower()


class FormatResultsFn(beam.DoFn):
    """Format word count results."""

    def process(self, element):
        """Format (word, count) tuple as text.

        Args:
            element: Tuple of (word, count)

        Yields:
            Formatted string
        """
        word, count = element
        yield f'{word}: {count}'


def run(input_path, output_path, pipeline_args=None):
    """Run the WordCount pipeline.

    Args:
        input_path: Path to input text file
        output_path: Path to output directory
        pipeline_args: Additional pipeline arguments
    """
    # Set up pipeline options
    pipeline_options = PipelineOptions(pipeline_args)

    # Create pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        (p
         # Read input file
         | 'Read' >> ReadFromText(input_path)

         # Extract words
         | 'ExtractWords' >> beam.ParDo(ExtractWordsFn())

         # Count word occurrences
         | 'PairWithOne' >> beam.Map(lambda word: (word, 1))
         | 'GroupAndSum' >> beam.CombinePerKey(sum)

         # Format results
         | 'Format' >> beam.ParDo(FormatResultsFn())

         # Write output
         | 'Write' >> WriteToText(output_path)
        )


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='data/shakespeare.txt',
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output file to write results to.')

    known_args, pipeline_args = parser.parse_known_args()

    run(known_args.input, known_args.output, pipeline_args)
