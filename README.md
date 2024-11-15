# Random Forest Models

#### Locally

Download mock S3 data for core and legacy as described in [Store](./store/README.md).

Start up an Arbimon mock DB and Store, seed it.

```bash
make serve-up
```

Run the RFM training job

```bash
docker compose exec app bash
JOB_ID=100001 python -m rfm.train_legacy
```

Run the RFM classification job

```bash
JOB_ID=100003 python -m rfm.classify_legacy
```

Run the RFM classification job on a legacy model (currently errors!)

```bash
JOB_ID=100002 python -m rfm.classify_legacy
```

#### Testing on production

Setup an ssh tunnel to the database (e.g. on port 3310)

```bash
sss -L3310:[DB_HOSTNAME]:3306 ec2-user@[BASTION_IP]
```

Run a job locally, use `FORCE_SEQUENTIAL_EXECUTION=1` for debugging

```bash
docker run \
      -v ${PWD}/rfm:/app/rfm \
      -e DB_HOST=host.docker.internal \
      -e DB_PORT=3310 \
      -e DB_NAME=arbimon2 \
      -e DB_USER=... \
      -e DB_PASSWORD=... \
      -e S3_BUCKET_NAME=rfcx-streams-production \
      -e S3_LEGACY_BUCKET_NAME=arbimon2 \
      -e AWS_ACCESS_KEY_ID=... \
      -e AWS_SECRET_ACCESS_KEY=... \
      -e FORCE_SEQUENTIAL_EXECUTION=1 \
      -e JOB_ID=110554 \
      rfm classify_legacy
```
