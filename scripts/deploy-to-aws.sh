#!/bin/bash
# Simple sequential deploy script - just logs outcomes
# Usage: ./scripts/deploy-to-aws-simple.sh <worker_storage_dir> [s3_bucket]

set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

WORKER_STORAGE_DIR="$1"
S3_BUCKET="${2:-555-public}"
SSH_KEY="$HOME/Downloads/amanMBP.pem"
REMOTE_BIN_DIR="/home/ec2-user/bin"
SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"

echo "=== Deploy Script Started ==="
echo "Worker storage: $WORKER_STORAGE_DIR"
echo "S3 bucket: $S3_BUCKET"
echo "SSH key: $SSH_KEY"
echo ""

# Step 1: Compile (excluding jobs folder - jobs are submitted separately as JARs)
echo "=== Step 1: Compiling ==="
rm -rf bin
mkdir -p bin

# Define library JAR paths
JSOUP_JAR="$PROJECT_ROOT/libs/jsoup-1.21.2.jar"
SNOWBALL_JAR="$PROJECT_ROOT/libs/libstemmer_java-3.0.1.jar"

# Check if libraries exist
if [ ! -f "$JSOUP_JAR" ]; then
  echo "Warning: JSoup JAR not found at $JSOUP_JAR"
fi
if [ ! -f "$SNOWBALL_JAR" ]; then
  echo "Warning: Snowball JAR not found at $SNOWBALL_JAR"
fi

# Compile all Java files except those in the jobs folder, with libraries in classpath
find src/cis5550 -name "*.java" ! -path "*/jobs/*" | xargs javac --release 21 -cp "$JSOUP_JAR:$SNOWBALL_JAR" -d bin

# Copy log.properties to bin so Logger can find it
cp src/log.properties bin/log.properties 2>/dev/null || true
# Also create src directory structure in bin for Logger compatibility
mkdir -p bin/src
cp src/log.properties bin/src/log.properties 2>/dev/null || true

# Copy libraries to bin directory so they're included in the zip
mkdir -p bin/libs
cp "$JSOUP_JAR" bin/libs/ 2>/dev/null || true
cp "$SNOWBALL_JAR" bin/libs/ 2>/dev/null || true

BIN_SIZE=$(du -sh bin | awk '{print $1}')
echo "Compiled. Size: $BIN_SIZE"
echo ""

# Step 1.5: Zip and upload to S3
echo "=== Step 1.5: Zipping and Uploading to S3 ==="
ZIP_FILE="bin-$(date +%s).zip"
cd bin
zip -r "../$ZIP_FILE" . > /dev/null 2>&1
cd "$PROJECT_ROOT"

# Move zip to bins_for_s3 directory
mkdir -p bins_for_s3
ZIP_FILENAME=$(basename "$ZIP_FILE")
mv "$ZIP_FILE" "bins_for_s3/$ZIP_FILENAME"
ZIP_FILE="bins_for_s3/$ZIP_FILENAME"
ZIP_SIZE=$(du -sh "$ZIP_FILE" | awk '{print $1}')
echo "Zipped. Size: $ZIP_SIZE"

echo "Uploading to S3..."
aws s3 cp "$ZIP_FILE" "s3://$S3_BUCKET/$ZIP_FILENAME" --acl public-read --region us-east-1
S3_URL="https://$S3_BUCKET.s3.us-east-1.amazonaws.com/$ZIP_FILENAME"
echo "Uploaded. URL: $S3_URL"
echo ""

# Step 2: Get Terraform outputs
echo "=== Step 2: Getting Terraform Outputs ==="
cd terraform

KVS_COORD_DNS=$(terraform output -raw kvs_coordinator_public_dns 2>/dev/null || terraform output kvs_coordinator_public_dns 2>/dev/null | head -1 | tr -d '"' || echo "")
KVS_COORD_IP=$(terraform output -raw kvs_coordinator_public_ip 2>/dev/null || terraform output kvs_coordinator_public_ip 2>/dev/null | head -1 | tr -d '"' || echo "")
KVS_COORD_PRIVATE_IP=$(terraform output -raw kvs_coordinator_private_ip 2>/dev/null || terraform output kvs_coordinator_private_ip 2>/dev/null | head -1 | tr -d '"' || echo "")

FLAME_COORD_DNS=$(terraform output -raw flame_coordinator_public_dns 2>/dev/null || terraform output flame_coordinator_public_dns 2>/dev/null | head -1 | tr -d '"' || echo "")
FLAME_COORD_IP=$(terraform output -raw flame_coordinator_public_ip 2>/dev/null || terraform output flame_coordinator_public_ip 2>/dev/null | head -1 | tr -d '"' || echo "")
FLAME_COORD_PRIVATE_IP=$(terraform output -raw flame_coordinator_private_ip 2>/dev/null || terraform output flame_coordinator_private_ip 2>/dev/null | head -1 | tr -d '"' || echo "")

WORKER_PUBLIC_IPS=$(terraform output -json kvs_worker_public_ips 2>/dev/null | jq -r '.[]' 2>/dev/null || terraform output kvs_worker_public_ips 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' || echo "")

cd "$PROJECT_ROOT"

KVS_COORD_HOST="${KVS_COORD_DNS:-$KVS_COORD_IP}"
FLAME_COORD_HOST="${FLAME_COORD_DNS:-$FLAME_COORD_IP}"

echo "KVS Coordinator: $KVS_COORD_HOST"
echo "Flame Coordinator: $FLAME_COORD_HOST"
echo "Workers: $(echo "$WORKER_PUBLIC_IPS" | wc -l | tr -d ' ')"
echo ""

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
echo "Timestamp: $TIMESTAMP"

# Step 3: Deploy to KVS Coordinator
echo "=== Step 3: Deploying to KVS Coordinator ==="
echo "Host: $KVS_COORD_HOST"
echo "Downloading and extracting bin from S3..."
ssh $SSH_OPTS ec2-user@"$KVS_COORD_HOST" "rm -rf $REMOTE_BIN_DIR && mkdir -p /home/ec2-user/logs && mkdir -p $REMOTE_BIN_DIR && cd /home/ec2-user && curl -s -o /tmp/bin.zip '$S3_URL' && unzip -q /tmp/bin.zip -d $REMOTE_BIN_DIR && rm /tmp/bin.zip" 2>/dev/null || true
echo "Bin downloaded and extracted"

BEFORE_KILL=$(ssh $SSH_OPTS ec2-user@"$KVS_COORD_HOST" "pgrep -f 'cis5550\.(kvs|flame)\.(Coordinator|Worker)' | wc -l" 2>/dev/null || echo "0")
echo "Processes before killing: $BEFORE_KILL"

echo "Killing processes..."
PIDS=$(ssh $SSH_OPTS ec2-user@"$KVS_COORD_HOST" "pgrep -f 'cis5550\.(kvs|flame)\.(Coordinator|Worker)'" 2>/dev/null | tr '\n' ' ' || echo "")
ssh $SSH_OPTS ec2-user@"$KVS_COORD_HOST" "for pid in $PIDS; do sudo kill \$pid 2>/dev/null || true; done; sleep 2; for pid in $PIDS; do sudo kill -9 \$pid 2>/dev/null || true; done" 2>/dev/null || true
AFTER_KILL=$(ssh $SSH_OPTS ec2-user@"$KVS_COORD_HOST" "pgrep -f 'cis5550\.(kvs|flame)\.(Coordinator|Worker)' | wc -l" 2>/dev/null || echo "0")
echo "Processes after killing: $AFTER_KILL"

echo "Starting KVS Coordinator (as root)..."
ssh -f $SSH_OPTS ec2-user@"$KVS_COORD_HOST" "mkdir -p /home/ec2-user/src && cp $REMOTE_BIN_DIR/src/log.properties /home/ec2-user/src/log.properties 2>/dev/null || cp $REMOTE_BIN_DIR/log.properties /home/ec2-user/src/log.properties 2>/dev/null || true && cd /home/ec2-user && sudo nohup sh -c '/usr/bin/java -cp \"$REMOTE_BIN_DIR:$REMOTE_BIN_DIR/libs/*\" cis5550.kvs.Coordinator 8000' >/home/ec2-user/logs/kvs-coordinator-${TIMESTAMP}.log 2>&1 &" 2>/dev/null || true
sleep 3
AFTER_START=$(ssh $SSH_OPTS ec2-user@"$KVS_COORD_HOST" "pgrep -f 'cis5550\.(kvs|flame)\.(Coordinator|Worker)' | wc -l" 2>/dev/null || echo "0")
echo "Processes after starting: $AFTER_START"
echo ""

# Step 4: Deploy to Flame Coordinator
echo "=== Step 4: Deploying to Flame Coordinator ==="
echo "Host: $FLAME_COORD_HOST"
echo "Downloading and extracting bin from S3..."
ssh $SSH_OPTS ec2-user@"$FLAME_COORD_HOST" "rm -rf $REMOTE_BIN_DIR && mkdir -p /home/ec2-user/logs && mkdir -p $REMOTE_BIN_DIR && cd /home/ec2-user && curl -s -o /tmp/bin.zip '$S3_URL' && unzip -q /tmp/bin.zip -d $REMOTE_BIN_DIR && rm /tmp/bin.zip" 2>/dev/null || true
echo "Bin downloaded and extracted"

BEFORE_KILL=$(ssh $SSH_OPTS ec2-user@"$FLAME_COORD_HOST" "pgrep -f 'cis5550\.(kvs|flame)\.(Coordinator|Worker)' | wc -l" 2>/dev/null || echo "0")
echo "Processes before killing: $BEFORE_KILL"

echo "Killing processes..."
PIDS=$(ssh $SSH_OPTS ec2-user@"$FLAME_COORD_HOST" "pgrep -f 'cis5550\.(kvs|flame)\.(Coordinator|Worker)'" 2>/dev/null | tr '\n' ' ' || echo "")
ssh $SSH_OPTS ec2-user@"$FLAME_COORD_HOST" "for pid in $PIDS; do sudo kill \$pid 2>/dev/null || true; done; sleep 2; for pid in $PIDS; do sudo kill -9 \$pid 2>/dev/null || true; done" 2>/dev/null || true
AFTER_KILL=$(ssh $SSH_OPTS ec2-user@"$FLAME_COORD_HOST" "pgrep -f 'cis5550\.(kvs|flame)\.(Coordinator|Worker)' | wc -l" 2>/dev/null || echo "0")
echo "Processes after killing: $AFTER_KILL"

echo "Starting Flame Coordinator (as root)..."
ssh -f $SSH_OPTS ec2-user@"$FLAME_COORD_HOST" "mkdir -p /home/ec2-user/src && cp $REMOTE_BIN_DIR/src/log.properties /home/ec2-user/src/log.properties 2>/dev/null || cp $REMOTE_BIN_DIR/log.properties /home/ec2-user/src/log.properties 2>/dev/null || true && cd /home/ec2-user && sudo nohup sh -c '/usr/bin/java -cp \"$REMOTE_BIN_DIR:$REMOTE_BIN_DIR/libs/*\" cis5550.flame.Coordinator 9000 $KVS_COORD_PRIVATE_IP:8000' >/home/ec2-user/logs/flame-coordinator-${TIMESTAMP}.log 2>&1 &" 2>/dev/null || true
sleep 3
AFTER_START=$(ssh $SSH_OPTS ec2-user@"$FLAME_COORD_HOST" "pgrep -f 'cis5550\.(kvs|flame)\.(Coordinator|Worker)' | wc -l" 2>/dev/null || echo "0")
echo "Processes after starting: $AFTER_START"
echo ""

# Function to generate deterministic worker IDs for even key distribution
generate_worker_id() {
  local WORKER_INDEX="$1"
  local TOTAL_WORKERS="$2"
  
  # Calculate position in 26^5 key space
  local POSITION=$(echo "scale=0; ($WORKER_INDEX * 11881376) / $TOTAL_WORKERS" | bc)
  
  # Convert to base-26 (5 characters)
  local ID=""
  local REMAINING=$POSITION
  
  for i in 4 3 2 1 0; do
    local POWER=$(echo "26^$i" | bc)
    local DIGIT=$(echo "scale=0; $REMAINING / $POWER" | bc)
    REMAINING=$(echo "scale=0; $REMAINING % $POWER" | bc)
    local CHAR=$(printf "\\$(printf '%03o' $((97 + DIGIT)))")
    ID="${ID}${CHAR}"
  done
  
  echo "$ID"
}


# Step 5: Deploy to Workers (PARALLEL)
echo "=== Step 5: Deploying to Workers (Parallel) ==="
WORKER_COUNT=$(echo "$WORKER_PUBLIC_IPS" | wc -l | tr -d ' ')
echo "Deploying to $WORKER_COUNT workers in parallel..."
echo "Worker IDs will be evenly distributed across key space for balanced load"
echo ""

# Function to deploy to a single worker
deploy_worker() {
  local WORKER_IP="$1"
  local WORKER_NUM="$2"
  local TOTAL_WORKERS="$3"
  
  echo "[Worker $WORKER_NUM] === Worker $WORKER_NUM: $WORKER_IP ==="
  
  echo "[Worker $WORKER_NUM] Downloading and extracting bin from S3..."
  ssh $SSH_OPTS ec2-user@"$WORKER_IP" "rm -rf $REMOTE_BIN_DIR && mkdir -p /home/ec2-user/logs && mkdir -p $REMOTE_BIN_DIR && cd /home/ec2-user && curl -s -o /tmp/bin.zip '$S3_URL' && unzip -q /tmp/bin.zip -d $REMOTE_BIN_DIR && rm /tmp/bin.zip" 2>/dev/null || true
  echo "[Worker $WORKER_NUM] Bin downloaded and extracted"
  
  echo "[Worker $WORKER_NUM] Setting up storage..."
  ssh $SSH_OPTS ec2-user@"$WORKER_IP" "sudo mkdir -p '$WORKER_STORAGE_DIR' && sudo chown ec2-user:ec2-user '$WORKER_STORAGE_DIR' && sudo chmod 755 '$WORKER_STORAGE_DIR'" 2>/dev/null || true
  echo "[Worker $WORKER_NUM] Storage ready: $WORKER_STORAGE_DIR"
  
  BEFORE_KILL=$(ssh $SSH_OPTS ec2-user@"$WORKER_IP" "pgrep -f 'cis5550\.(kvs|flame)\.(Coordinator|Worker)' | wc -l" 2>/dev/null || echo "0")
  echo "[Worker $WORKER_NUM] Processes before killing: $BEFORE_KILL"
  
  echo "[Worker $WORKER_NUM] Killing processes..."
  PIDS=$(ssh $SSH_OPTS ec2-user@"$WORKER_IP" "pgrep -f 'cis5550\.(kvs|flame)\.(Coordinator|Worker)'" 2>/dev/null | tr '\n' ' ' || echo "")
  ssh $SSH_OPTS ec2-user@"$WORKER_IP" "for pid in $PIDS; do sudo kill \$pid 2>/dev/null || true; done; sleep 2; for pid in $PIDS; do sudo kill -9 \$pid 2>/dev/null || true; done" 2>/dev/null || true
  AFTER_KILL=$(ssh $SSH_OPTS ec2-user@"$WORKER_IP" "pgrep -f 'cis5550\.(kvs|flame)\.(Coordinator|Worker)' | wc -l" 2>/dev/null || echo "0")
  echo "[Worker $WORKER_NUM] Processes after killing: $AFTER_KILL"
  
  # Generate deterministic worker ID based on worker index
  # This ensures even distribution across the key space
  WORKER_INDEX=$((WORKER_NUM - 1))  # Convert to 0-based index
  WORKER_ID=$(generate_worker_id "$WORKER_INDEX" "$TOTAL_WORKERS")
  echo "[Worker $WORKER_NUM] Generated deterministic Worker ID: $WORKER_ID (ensures even key distribution)"
  
  # Calculate Flame port: 9001-9021 based on worker index (matches terraform count.index)
  FLAME_PORT=$((9001 + WORKER_INDEX))
  if [ $FLAME_PORT -gt 9021 ]; then
    FLAME_PORT=$((9001 + (WORKER_INDEX % 21)))
  fi
  
  echo "[Worker $WORKER_NUM] Starting Flame Worker on port $FLAME_PORT (as root)..."
  ssh -f $SSH_OPTS ec2-user@"$WORKER_IP" "mkdir -p /home/ec2-user/src && cp $REMOTE_BIN_DIR/src/log.properties /home/ec2-user/src/log.properties 2>/dev/null || cp $REMOTE_BIN_DIR/log.properties /home/ec2-user/src/log.properties 2>/dev/null || true && cd /home/ec2-user && sudo nohup sh -c '/usr/bin/java -cp \"$REMOTE_BIN_DIR:$REMOTE_BIN_DIR/libs/*\" cis5550.flame.Worker $FLAME_PORT $FLAME_COORD_PRIVATE_IP:9000' >/home/ec2-user/logs/flame-worker-${TIMESTAMP}.log 2>&1 &" 2>/dev/null || true
  sleep 2
  
  echo "[Worker $WORKER_NUM] Starting KVS Worker with ID $WORKER_ID (as root)..."
  ssh -f $SSH_OPTS ec2-user@"$WORKER_IP" "mkdir -p /home/ec2-user/src && cp $REMOTE_BIN_DIR/src/log.properties /home/ec2-user/src/log.properties 2>/dev/null || cp $REMOTE_BIN_DIR/log.properties /home/ec2-user/src/log.properties 2>/dev/null || true && cd /home/ec2-user && sudo nohup sh -c '/usr/bin/java -cp \"$REMOTE_BIN_DIR:$REMOTE_BIN_DIR/libs/*\" cis5550.kvs.Worker 8001 \"$WORKER_STORAGE_DIR\" $KVS_COORD_PRIVATE_IP:8000 $WORKER_ID' >/home/ec2-user/logs/kvs-worker-${TIMESTAMP}.log 2>&1 &" 2>/dev/null || true
  sleep 2
  
  AFTER_START=$(ssh $SSH_OPTS ec2-user@"$WORKER_IP" "pgrep -f 'cis5550\.(kvs|flame)\.(Coordinator|Worker)' | wc -l" 2>/dev/null || echo "0")
  KVS_RUNNING=$(ssh $SSH_OPTS ec2-user@"$WORKER_IP" "pgrep -f 'cis5550.kvs.Worker' | wc -l" 2>/dev/null || echo "0")
  FLAME_RUNNING=$(ssh $SSH_OPTS ec2-user@"$WORKER_IP" "pgrep -f 'cis5550.flame.Worker' | wc -l" 2>/dev/null || echo "0")
  
  echo "[Worker $WORKER_NUM] Processes after starting: $AFTER_START"
  echo "[Worker $WORKER_NUM] KVS Worker running: $KVS_RUNNING"
  echo "[Worker $WORKER_NUM] Flame Worker running: $FLAME_RUNNING"
  echo "[Worker $WORKER_NUM] === Worker $WORKER_NUM complete ==="
}

# Export variables and function for subshells
export SSH_OPTS SSH_KEY REMOTE_BIN_DIR S3_URL WORKER_STORAGE_DIR KVS_COORD_PRIVATE_IP FLAME_COORD_PRIVATE_IP TIMESTAMP WORKER_COUNT
export -f deploy_worker
export -f generate_worker_id

# Launch all workers in parallel
PIDS=""
WORKER_NUM=0
for WORKER_IP in $WORKER_PUBLIC_IPS; do
  WORKER_NUM=$((WORKER_NUM + 1))
  deploy_worker "$WORKER_IP" "$WORKER_NUM" "$WORKER_COUNT" &
  PIDS="$PIDS $!"
done

# Wait for all workers to complete
echo "Waiting for all $WORKER_COUNT workers to complete..."
for PID in $PIDS; do
  wait $PID 2>/dev/null || true
done

echo ""
echo "=== Deployment Complete ==="
echo "All instances processed with deterministic worker IDs"
echo "Worker IDs are evenly distributed: $(generate_worker_id 0 $WORKER_COUNT) to $(generate_worker_id $((WORKER_COUNT - 1)) $WORKER_COUNT)"
echo "This ensures balanced key distribution across all workers"
echo "Check logs in /home/ec2-user/logs/ on each instance"
echo ""
echo "Cleaning up local zip file..."
rm -f "$ZIP_FILE"
echo "Done"