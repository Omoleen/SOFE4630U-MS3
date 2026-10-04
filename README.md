# SOFE4630U Milestone 3: Data Processing (Dataflow)

**Name:** Emmanuel Omole  
**Student number:** 101004432

GCP project: `clarity-staging-4afe8`, region `northamerica-northeast2`.

## Layout

| Path | Purpose |
| --- | --- |
| `Design/csvProducer.py` | Reads `Labels.csv` and publishes each record as JSON to `weatherLabels`. |
| `Design/weatherPipeline.py` | Streaming Dataflow job. Reads, filters out incomplete records, converts units, writes to `weatherConverted`. |
| `Design/csvConsumer.py` | Consumes `weatherConverted-sub` and prints each processed record. |
| `Design/Labels.csv` | 100 weather records, 22 of them with a missing value. |

## Design pipeline

```
csvProducer.py -> weatherLabels -> [weatherLabels-dataflow]
                                          |
                                Dataflow: weatherPipeline.py
            Read from PubSub -> toDict -> Filter -> Convert -> toBytes -> Write to PubSub
                                          |
                                          v
                   weatherConverted -> [weatherConverted-sub] -> csvConsumer.py
```

- **Filter** drops a record if any field is `None`.
- **Convert** sets `pressure = pressure / 6.895` (kPa to psi) and
  `temperature = temperature * 1.8 + 32` (C to F). Humidity is unchanged.

## Running

1. Create the topics and subscriptions:

   ```shell
   gcloud pubsub subscriptions create weatherLabels-dataflow --topic weatherLabels
   gcloud pubsub topics create weatherConverted
   gcloud pubsub subscriptions create weatherConverted-sub --topic weatherConverted
   ```

2. Install the libraries:

   ```shell
   pip install 'apache-beam[gcp]' google-cloud-pubsub
   ```

3. Start the Dataflow job (it runs until cancelled):

   ```shell
   PROJECT=$(gcloud config list project --format "value(core.project)")
   BUCKET=gs://$PROJECT-bucket
   cd Design
   python weatherPipeline.py \
     --runner DataflowRunner \
     --project $PROJECT \
     --region northamerica-northeast2 \
     --job_name weather-preprocess \
     --staging_location $BUCKET/staging \
     --temp_location $BUCKET/temp \
     --input projects/$PROJECT/subscriptions/weatherLabels-dataflow \
     --output projects/$PROJECT/topics/weatherConverted \
     --experiment use_unsupported_python_version
   ```

4. Copy the service account JSON key into `Design/`, then in two terminals:

   ```shell
   python csvConsumer.py     # terminal 1
   python csvProducer.py     # terminal 2
   ```

5. Stop the job from the Dataflow Jobs page, or:

   ```shell
   gcloud dataflow jobs cancel JOB_ID --region northamerica-northeast2
   ```

The key file is excluded by `.gitignore` and is not part of this repository.
