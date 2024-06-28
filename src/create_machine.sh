#!/bin/bash
gcloud compute instances create local-run \
    --machine-type=n1-highmem-16 \
    --zone=us-central1-a \
    --subnet=default \
    --network-tier=PREMIUM \
    --maintenance-policy=MIGRATE \
    --image=ubuntu-2004-focal-v20240614 \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=200GB \
    --boot-disk-type=pd-ssd \
