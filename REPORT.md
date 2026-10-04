# SOFE4630U Milestone 3 Report
## Data Processing with Google Cloud Dataflow

**Name:** Emmanuel Omole  
**Student number:** 101004432  
**Date:** 2026-10-04  

**GitHub repository:** https://github.com/Omoleen/SOFE4630U-MS3

---

## 1. Environment

| Item | Value |
| --- | --- |
| GCP project | `clarity-staging-4afe8` |
| Region | `northamerica-northeast2` |
| Service account | `clarity-staging-4afe8-dfsa`, Compute Engine Service Agent and Pub/Sub Admin, plus Dataflow, Storage and BigQuery roles to submit jobs |
| Bucket | `gs://clarity-staging-4afe8-bucket`, public access prevention on |
| Python / Beam | 3.12, `apache-beam[gcp]` 2.76.0 |

## 2. Lab examples

| Example | Job | Result |
| --- | --- | --- |
| 1. `wordcount.py` | Batch | 1347 distinct words of `winterstale.txt` in `result/wordcount/outputs-*`. The most frequent is `and` (662). |
| 2. `wordcount2.py` | Batch, two branches | `outputs`: counts of lowercase words starting with a to f. `outputs2`: counts per first letter, `t` highest at 3664. |
| 3. `mnistBQ.py` | Batch, BigQuery to BigQuery | 10000 rows read from `MNIST.Images`, 10000 rows of `ID, P0..P9` written to `MNIST.Predict`. |
| 4. `mnistPubSub.py` | Streaming, Pub/Sub to Pub/Sub | Producer sent images to `mnist_image`, consumer printed predictions from `mnist_predict-sub`. Job cancelled after the test. |

`wordcount.py` was first run locally to check the pipeline before submitting it
to Dataflow. Both MNIST jobs install TensorFlow on each worker through
`setup.py`, so their workers take several minutes longer to start than the
word count jobs.

## 3. Design

### 3.1 Pipeline

```
csvProducer.py -> weatherLabels -> [weatherLabels-dataflow]
                                          |
                                Dataflow: weatherPipeline.py
            Read from PubSub -> toDict -> Filter -> Convert -> toBytes -> Write to PubSub
                                          |
                                          v
                   weatherConverted -> [weatherConverted-sub] -> csvConsumer.py
```

### 3.2 Steps taken

1. Reused `csvProducer.py` from Milestone 1. It publishes each row of
   `Labels.csv` as JSON to `weatherLabels`, with empty cells sent as `null`.
2. Created a subscription `weatherLabels-dataflow` on that topic, plus the
   output topic `weatherConverted` and its subscription `weatherConverted-sub`.
3. Wrote `weatherPipeline.py`, a streaming Beam pipeline with these stages:
   - **Read from PubSub** reads raw messages from `weatherLabels-dataflow`.
   - **toDict** decodes the bytes and parses the JSON into a dictionary.
   - **Filter** keeps a record only if no field is `None`.
   - **Convert** sets `pressure = pressure / 6.895` (kPa to psi) and
     `temperature = temperature * 1.8 + 32` (C to F). Humidity, time and
     profile name pass through unchanged.
   - **toBytes** serializes the record back to JSON bytes.
   - **Write to PubSub** publishes it to `weatherConverted`.
4. Launched the job on Dataflow with the `DataflowRunner`, using the bucket for
   staging and temporary files.
5. Pointed the Milestone 1 consumer at `weatherConverted-sub` and ran it, then
   ran the producer.

### 3.3 Choices

**Subscription instead of topic as input.** `ReadFromPubSub(topic=...)` makes a
temporary subscription when the job starts, so records published before the
workers are ready are lost. A named subscription holds them until the job reads
them, so the producer can run at any time.

**Filter before Convert.** The conversion does arithmetic on the readings, which
fails on `None`. Filtering first means Convert only ever sees complete records.

**Streaming.** Pub/Sub is unbounded, so the pipeline sets `streaming = True` in
code. It runs until it is cancelled from the Dataflow Jobs page.

### 3.4 Result

The producer published 100 records. 22 had a missing value, and the consumer
received the other 78. Each of the 78 was checked against `Labels.csv`: every
temperature and pressure matched the formulas, and humidity was unchanged.

```
Consumed record:
   time : 1768708698.4968069
   profileName : denver
   temperature : 83.4539755701463
   humidity : 42.198609009864995
   pressure : 0.1470887970319075
```
