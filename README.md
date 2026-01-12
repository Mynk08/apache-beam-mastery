# 🌊 Apache Beam Mastery: Complete Learning Path

[![Apache Beam](https://img.shields.io/badge/Apache%20Beam-2.54-orange)](https://beam.apache.org/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Java 11+](https://img.shields.io/badge/Java-11+-red.svg)](https://www.oracle.com/java/)

> **The ultimate learning repository for mastering Apache Beam - from zero to production. Comprehensive tutorials, real-world examples, and hands-on projects covering batch & streaming data processing.**

## 📚 What is Apache Beam?

**Apache Beam** is a unified programming model for defining and executing data processing pipelines, including:
- **Batch processing** (bounded data)
- **Stream processing** (unbounded data)
- **Hybrid workflows** (combining both)

### Key Concepts
```
Pipeline → PCollection → Transforms → Runners
    ↓          ↓            ↓           ↓
  Workflow   Data      Operations   Execution
```

## 🎯 Learning Path

### Level 1: Foundations (Weeks 1-2)
- [ ] [Understanding Beam Model](docs/01-beam-model.md)
- [ ] [Core Concepts: PCollection & PTransforms](docs/02-core-concepts.md)
- [ ] [Your First Pipeline](tutorials/01-hello-beam/)
- [ ] [Windowing & Triggers Basics](tutorials/02-windowing/)

### Level 2: Intermediate (Weeks 3-4)
- [ ] [Advanced Transformations](tutorials/03-advanced-transforms/)
- [ ] [State & Timers](tutorials/04-state-timers/)
- [ ] [Side Inputs & Outputs](tutorials/05-side-io/)
- [ ] [Error Handling & Dead Letter Queues](tutorials/06-error-handling/)

### Level 3: Advanced (Weeks 5-6)
- [ ] [Custom DoFns & Splittable DoFns](tutorials/07-custom-dofn/)
- [ ] [Metrics & Monitoring](tutorials/08-metrics/)
- [ ] [Performance Tuning](tutorials/09-performance/)
- [ ] [Testing Strategies](tutorials/10-testing/)

### Level 4: Production (Weeks 7-8)
- [ ] [Running on DataflowRunner](docs/runners/dataflow.md)
- [ ] [Running on Flink](docs/runners/flink.md)
- [ ] [Running on Spark](docs/runners/spark.md)
- [ ] [CI/CD for Beam Pipelines](docs/cicd/)

## 🚀 Quick Start

### Python Setup

```bash
# Clone repository
git clone https://github.com/Mynk08/apache-beam-mastery.git
cd apache-beam-mastery

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install Apache Beam
pip install apache-beam[gcp,aws,azure]

# Run your first pipeline
python examples/01-word-count/wordcount.py \
  --input data/shakespeare.txt \
  --output output/counts
```

### Java Setup

```bash
# Prerequisites: Java 11+, Maven 3.6+
mvn archetype:generate \
  -DarchetypeGroupId=org.apache.beam \
  -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-examples \
  -DarchetypeVersion=2.54.0

# Run WordCount example
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
  --input=data/shakespeare.txt --output=counts
```

## 📖 Project Structure

```
apache-beam-mastery/
├── docs/                    # Comprehensive documentation
│   ├── 01-beam-model.md
│   ├── 02-core-concepts.md
│   ├── runners/             # Runner-specific guides
│   │   ├── direct.md
│   │   ├── dataflow.md
│   │   ├── flink.md
│   │   └── spark.md
│   └── architecture/        # Deep dives
├── tutorials/               # Step-by-step tutorials
│   ├── 01-hello-beam/
│   ├── 02-windowing/
│   ├── 03-advanced-transforms/
│   └── ...
├── examples/                # Real-world examples
│   ├── 01-word-count/
│   ├── 02-streaming-analytics/
│   ├── 03-ml-pipeline/
│   ├── 04-etl-pipeline/
│   └── 05-real-time-dashboard/
├── projects/                # Hands-on projects
│   ├── twitter-sentiment/
│   ├── log-analyzer/
│   └── recommendation-engine/
├── tests/                   # Testing patterns
└── benchmarks/              # Performance benchmarks
```

## 🔥 Featured Examples

### 1. Real-time Twitter Sentiment Analysis
```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

with beam.Pipeline(options=PipelineOptions()) as p:
    (p 
     | 'Read from Twitter' >> beam.io.ReadFromPubSub(topic='tweets')
     | 'Extract text' >> beam.Map(lambda x: x['text'])
     | 'Analyze sentiment' >> beam.ParDo(SentimentAnalyzer())
     | 'Window' >> beam.WindowInto(beam.window.FixedWindows(60))
     | 'Aggregate' >> beam.CombinePerKey(beam.combiners.MeanCombineFn())
     | 'Write to BigQuery' >> beam.io.WriteToBigQuery(table_spec)
    )
```

### 2. ML Feature Engineering Pipeline
```python
class FeatureExtractor(beam.DoFn):
    def process(self, element):
        features = {
            'user_id': element['user_id'],
            'hour_of_day': element['timestamp'].hour,
            'day_of_week': element['timestamp'].weekday(),
            'transaction_amount': element['amount'],
            # ... more features
        }
        yield features

with beam.Pipeline() as p:
    (p
     | 'Read from Kafka' >> ReadFromKafka(consumer_config)
     | 'Extract features' >> beam.ParDo(FeatureExtractor())
     | 'Normalize' >> beam.ParDo(Normalizer())
     | 'Write to Vertex AI' >> WriteToVertexAI(model_id)
    )
```

### 3. ETL Pipeline with Error Handling
```python
good_records, bad_records = (
    p
    | 'Read CSV' >> beam.io.ReadFromText('gs://bucket/data.csv')
    | 'Parse' >> beam.ParDo(ParseCsv()).with_outputs('bad', main='good')
)

(good_records 
 | 'Transform' >> beam.Map(transform_record)
 | 'Write to Warehouse' >> beam.io.WriteToBigQuery(...)
)

(bad_records
 | 'Write to DLQ' >> beam.io.WriteToText('gs://bucket/errors/')
)
```

## 🏗️ Runners Comparison

| Runner | Use Case | Pros | Cons |
|--------|----------|------|------|
| **DirectRunner** | Local testing | Fast iteration, easy debugging | Not for production, no distribution |
| **DataflowRunner** | GCP production | Fully managed, autoscaling | GCP-only, cost |
| **FlinkRunner** | On-prem/streaming | Mature, low latency | Complex setup, ops overhead |
| **SparkRunner** | Existing Spark infra | Leverage Spark ecosystem | Higher latency than Flink |

## 💡 Best Practices

### 1. Pipeline Design
- ✅ Keep transforms stateless when possible
- ✅ Use side inputs for lookup tables
- ✅ Implement idempotent writes
- ✅ Add comprehensive error handling
- ❌ Don't perform I/O in transforms
- ❌ Avoid large shuffle operations

### 2. Performance Optimization
```python
# Bad: Inefficient grouping
(p | beam.Map(lambda x: (x['key'], x))
   | beam.GroupByKey()
   | beam.Map(sum_values))

# Good: Use CombinePerKey
(p | beam.Map(lambda x: (x['key'], x['value']))
   | beam.CombinePerKey(sum))
```

### 3. Testing
```python
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

def test_word_count():
    with TestPipeline() as p:
        input = p | beam.Create(['hello world', 'hello beam'])
        output = input | CountWords()

        assert_that(output, equal_to([('hello', 2), ('world', 1), ('beam', 1)]))
```

## 🎓 Learning Resources

### Official Documentation
- [Apache Beam Website](https://beam.apache.org/)
- [Programming Guide](https://beam.apache.org/documentation/programming-guide/)
- [Python SDK](https://beam.apache.org/documentation/sdks/python/)
- [Java SDK](https://beam.apache.org/documentation/sdks/java/)

### Video Tutorials
- [Beam Summit 2023](https://www.youtube.com/watch?v=...) - Latest features
- [Google Cloud Next](https://www.youtube.com/watch?v=...) - Dataflow deep dives

### Books
- "Streaming Systems" by Tyler Akidau et al. (O'Reilly)
- "Apache Beam in Action" (Manning)

## 🤝 Contributing to Apache Beam

### Finding Issues
```bash
# Good first issues
https://github.com/apache/beam/labels/good%20first%20issue

# Starter issues
https://github.com/apache/beam/labels/starter

# Help wanted
https://github.com/apache/beam/labels/help%20wanted
```

### Contribution Workflow
1. **Find an issue** from labels above
2. **Set up dev environment**:
   ```bash
   git clone https://github.com/apache/beam.git
   cd beam
   ./gradlew build  # For Java
   # or
   pip install -e ./sdks/python  # For Python
   ```
3. **Make changes** and add tests
4. **Submit PR** following [contribution guide](https://beam.apache.org/contribute/)

### Areas to Contribute
- 📝 Documentation improvements
- 🐛 Bug fixes in runners
- ✨ New I/O connectors
- 🧪 Test coverage
- 📊 Example pipelines

## 🔧 Development Setup

### Python Development
```bash
# Install in editable mode
git clone https://github.com/apache/beam.git
cd beam/sdks/python
pip install -e .[gcp,test]

# Run tests
python setup.py test

# Run specific test
pytest apache_beam/transforms/window_test.py::WindowTest
```

### Java Development
```bash
# Build from source
./gradlew build

# Run specific module tests
./gradlew :sdks:java:core:test

# Run integration tests
./gradlew :runners:google-cloud-dataflow-java:integrationTest
```

## 📊 Real-world Use Cases

### 1. Log Processing Pipeline
- **Input**: Application logs from Cloud Logging
- **Processing**: Parse, filter, aggregate metrics
- **Output**: Elasticsearch + alerting

### 2. Event-driven Architecture
- **Input**: Kafka/Pub/Sub events
- **Processing**: Stateful processing, enrichment
- **Output**: BigQuery, downstream systems

### 3. ML Training Data Pipeline
- **Input**: Raw user interactions
- **Processing**: Feature engineering, sampling
- **Output**: TFRecords for training

## 🏆 Certifications

- **Google Professional Data Engineer** - Covers Dataflow/Beam
- **Confluent Certified Developer** - Kafka + Beam integration

## 📈 Roadmap

- [x] Core tutorials (Levels 1-2)
- [ ] Advanced examples (Level 3)
- [ ] Production patterns (Level 4)
- [ ] Multi-language pipelines
- [ ] Beam SQL deep dive
- [ ] Cross-language transforms
- [ ] Beam ML integration

## 💬 Community

- **Slack**: [Apache Beam Workspace](https://s.apache.org/beam-slack-channel)
- **Mailing List**: [dev@beam.apache.org](mailto:dev@beam.apache.org)
- **Stack Overflow**: [apache-beam](https://stackoverflow.com/questions/tagged/apache-beam)
- **Twitter**: [@ApacheBeam](https://twitter.com/ApacheBeam)

## 📝 License

This project is licensed under Apache License 2.0 - see [LICENSE](LICENSE)

## 🙏 Acknowledgments

- Apache Beam community
- Google Cloud Dataflow team
- All contributors

---

**⭐ Star this repo if you found it helpful!**

**🔗 Official Apache Beam**: https://beam.apache.org/
**📖 Beam Programming Guide**: https://beam.apache.org/documentation/programming-guide/

---

Made with ❤️ by the data engineering community
