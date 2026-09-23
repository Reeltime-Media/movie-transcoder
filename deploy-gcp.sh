#!/usr/bin/env bash
# One-shot provisioning + deploy of the movie-transcoder service to a GCE VM.
# Run from anywhere: bash deploy-gcp.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PROJECT_ID="project-cad9b658-276b-47e1-adc"  # Reeltime-v4
ACCOUNT="thychanna17@gmail.com"
BILLING_ACCOUNT="01539B-983BA6-315B4F"
ZONE="asia-southeast1-a"
REGION="asia-southeast1"
INSTANCE_NAME="movie-transcoder"
MACHINE_TYPE="e2-medium"
REPO_URL="https://github.com/Reeltime-Media/movie-transcoder.git"
ENV_FILE="$SCRIPT_DIR/.env"
PORT=8001

if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: $ENV_FILE not found." >&2
  exit 1
fi

gcloud config set account "$ACCOUNT" --quiet
gcloud config set project "$PROJECT_ID" --quiet

echo "==> Checking billing is linked"
BILLING_ENABLED=$(gcloud billing projects describe "$PROJECT_ID" --format='value(billingEnabled)')
if [[ "$BILLING_ENABLED" != "True" ]]; then
  echo "    linking billing account $BILLING_ACCOUNT"
  gcloud billing projects link "$PROJECT_ID" --billing-account="$BILLING_ACCOUNT"
else
  echo "    billing already enabled"
fi

echo "==> Enabling Compute Engine API"
gcloud services enable compute.googleapis.com --quiet

echo "==> Ensuring firewall rule for tcp:$PORT (open to 0.0.0.0/0)"
if ! gcloud compute firewall-rules describe allow-transcoder-8001 &>/dev/null; then
  gcloud compute firewall-rules create allow-transcoder-8001 \
    --network=default \
    --direction=INGRESS \
    --action=ALLOW \
    --rules="tcp:${PORT}" \
    --source-ranges="0.0.0.0/0" \
    --target-tags=transcoder
else
  echo "    firewall rule already exists, skipping"
fi

echo "==> Creating VM $INSTANCE_NAME in $ZONE (skip if it already exists)"
if ! gcloud compute instances describe "$INSTANCE_NAME" --zone="$ZONE" &>/dev/null; then
  gcloud compute instances create "$INSTANCE_NAME" \
    --zone="$ZONE" \
    --machine-type="$MACHINE_TYPE" \
    --image-family=debian-12 \
    --image-project=debian-cloud \
    --tags=transcoder
else
  echo "    instance already exists, skipping create"
fi

echo "==> Waiting for SSH to become available"
for i in $(seq 1 20); do
  if gcloud compute ssh "$INSTANCE_NAME" --zone="$ZONE" --quiet --command="echo ready" &>/dev/null; then
    echo "    SSH is up"
    break
  fi
  echo "    not ready yet, retrying in 10s ($i/20)"
  sleep 10
done

echo "==> Copying .env to the VM"
gcloud compute scp "$ENV_FILE" "$INSTANCE_NAME:~/transcoder.env" --zone="$ZONE" --quiet

echo "==> Kicking off install/build/run on the VM in the background"
gcloud compute ssh "$INSTANCE_NAME" --zone="$ZONE" --quiet --command="
  rm -f ~/deploy.log
  nohup bash -c '
    set -e
    if ! command -v docker &>/dev/null; then
      sudo apt-get update -y
      sudo apt-get install -y docker.io git
      sudo systemctl enable --now docker
    fi
    if [ -d ~/app ]; then
      (cd ~/app && sudo git pull)
    else
      sudo git clone \"$REPO_URL\" ~/app
    fi
    sudo docker build -t movie-transcoder ~/app
    sudo docker rm -f transcoder 2>/dev/null || true
    sudo docker run -d --name transcoder --env-file ~/transcoder.env -p ${PORT}:${PORT} --restart unless-stopped movie-transcoder
  ' > ~/deploy.log 2>&1 < /dev/null &
  disown
  echo started
"

echo "==> Polling for the container to come up (this can take a few minutes, output stays on the VM)"
for i in $(seq 1 60); do
  STATUS=$(gcloud compute ssh "$INSTANCE_NAME" --zone="$ZONE" --quiet --command="sudo docker ps --filter name=transcoder --format '{{.Status}}' 2>/dev/null" 2>/dev/null || true)
  if [[ -n "$STATUS" ]]; then
    echo "    container is up: $STATUS"
    break
  fi
  echo "    still building/starting... ($i/60) - tail: $(gcloud compute ssh "$INSTANCE_NAME" --zone="$ZONE" --quiet --command="tail -n 1 ~/deploy.log 2>/dev/null" 2>/dev/null || true)"
  sleep 15
done

EXTERNAL_IP=$(gcloud compute instances describe "$INSTANCE_NAME" --zone="$ZONE" --format='get(networkInterfaces[0].accessConfigs[0].natIP)')

echo ""
echo "=================================================="
echo " Transcoder hosted at: http://${EXTERNAL_IP}:${PORT}"
echo " Full remote build log: ~/deploy.log on the VM"
echo "=================================================="
