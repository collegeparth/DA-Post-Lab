# DA Post Lab

This repository contains a practical Big Data Analytics lab model based on the
5 V's of Big Data: volume, velocity, variety, veracity, and value.

## Contents

- `script.py` builds a PySpark Structured Streaming pipeline using Spark's rate
  source. It assigns simulated events to multiple channels, flags anomalous
	values, and calculates windowed aggregates and generated value.
- `index.html` provides a browser-based simulation dashboard for observing the
  streaming model and its metrics.

## PySpark Lab

### Requirements

- Python 3.9+
- Apache Spark with PySpark
- Java 8 or later, as required by the installed Spark version

Install PySpark with:

```bash
pip install pyspark
```

Run the streaming script with:

```bash
python script.py
```

The script starts a rate stream at 50 rows per second, groups events into
5-second windows by source channel, prints the results to the console, and
stops after approximately 20 seconds.

## Dashboard

Open `index.html` in a modern browser. The dashboard is a front-end simulation
and loads Tailwind CSS, Chart.js, and Font Awesome from public CDNs, so an
internet connection is needed when opening it for the first time.

Use the dashboard controls to pause, resume, or reset the simulated stream.

## Lab Focus

The exercise demonstrates how streaming data can be ingested, enriched,
validated, aggregated over time windows, and translated into operational
insights.