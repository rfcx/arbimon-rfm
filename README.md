# Random Forest Models

#### Locally

Download mock S3 data as described in [Store](./store/README.md). Core mock data is sufficient.

Copy `example.env` to `.env` and define your rfm parameters. (Project 1907 is a default that is defined in the mock db and store, it has recordings in 2020 and 2022.)

Start up an Arbimon mock DB and Store, seed it.

```bash
make serve-up
```

Run the RFM training job (`rfm/train_legacy.py`).

```bash
make serve-run SCRIPT=train_legacy
```

Inspect the results in mock store.

```bash
make serve-run SCRIPT="s3_get project_1907"
```

When you have finished

```bash
make serve-down
```
