#!/bin/bash
# Deploy Search Server to AWS on KVS Coordinator instance
# Usage: ./scripts/deploy-search-server-aws.sh [terraform_dir] [s3_bucket]
#   terraform_dir: Terraform directory to use (default: terraform)
#   s3_bucket: S3 bucket for uploading binaries (default: 555-public)
#   Example: ./scripts/deploy-search-server-aws.sh terraform 555-public

set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

# Get terraform directory and S3 bucket from arguments or use defaults
TERRAFORM_DIR="${1:-terraform}"
S3_BUCKET="${2:-555-public}"
SSH_KEY="$HOME/Downloads/amanMBP.pem"
REMOTE_BASE_DIR="/home/ec2-user/searchengine"
REMOTE_BIN_DIR="/home/ec2-user/searchengine/bin"
REMOTE_LIBS_DIR="/home/ec2-user/searchengine/libs"
REMOTE_LOGS_DIR="/home/ec2-user/searchengine/logs"
REMOTE_SRC_DIR="/home/ec2-user/searchengine/src"
SSH_OPTS="-i $SSH_KEY"
SEARCH_PORT=10000
KVS_COORD_PORT=8000

echo "==================================="
echo "Deploying Search Server to AWS"
echo "==================================="
echo "Terraform directory: $TERRAFORM_DIR"
echo "S3 bucket: $S3_BUCKET"
echo "Search server port: $SEARCH_PORT"
echo "KVS coordinator: localhost:$KVS_COORD_PORT"
echo ""

# Check SSH key exists
if [ ! -f "$SSH_KEY" ]; then
    echo "ERROR: SSH key not found at $SSH_KEY"
    echo "Please update SSH_KEY variable in the script or create the key file"
    exit 1
fi

# Step 1: Get Terraform outputs
echo "=== Step 1: Getting Terraform Outputs ==="
cd "$TERRAFORM_DIR"

KVS_COORD_DNS=$(terraform output -raw kvs_coordinator_public_dns 2>/dev/null || terraform output kvs_coordinator_public_dns 2>/dev/null | head -1 | tr -d '"' || echo "")
KVS_COORD_IP=$(terraform output -raw kvs_coordinator_public_ip 2>/dev/null || terraform output kvs_coordinator_public_ip 2>/dev/null | head -1 | tr -d '"' || echo "")

cd "$PROJECT_ROOT"

KVS_COORD_HOST="${KVS_COORD_DNS:-$KVS_COORD_IP}"

if [ -z "$KVS_COORD_HOST" ]; then
    echo "ERROR: Could not get KVS coordinator host from Terraform directory: $TERRAFORM_DIR"
    echo "Make sure 'terraform apply' has been run in $TERRAFORM_DIR and outputs are available."
    exit 1
fi

echo "KVS Coordinator: $KVS_COORD_HOST"
echo ""

# Step 2: Compile with jsoup and snowball
echo "=== Step 2: Compiling Source Files ==="
JSOUP_JAR="$PROJECT_ROOT/libs/jsoup-1.21.2.jar"
SNOWBALL_JAR="$PROJECT_ROOT/libs/libstemmer_java-3.0.1.jar"

if [ ! -f "$JSOUP_JAR" ] || [ ! -f "$SNOWBALL_JAR" ]; then
    echo "ERROR: JAR files not found:"
    [ ! -f "$JSOUP_JAR" ] && echo "  Missing: $JSOUP_JAR"
    [ ! -f "$SNOWBALL_JAR" ] && echo "  Missing: $SNOWBALL_JAR"
    exit 1
fi

# Clean and compile
rm -rf bin
mkdir -p bin
echo "Compiling all Java source files (with jsoup and snowball)..."
javac --release 21 -cp "$JSOUP_JAR:$SNOWBALL_JAR" -d bin src/cis5550/**/*.java 2>&1 | head -30
if [ ${PIPESTATUS[0]} -ne 0 ]; then
    echo "ERROR: Compilation failed!"
    exit 1
fi

# Copy log.properties to bin so Logger can find it
cp src/log.properties bin/log.properties 2>/dev/null || true
mkdir -p bin/src
cp src/log.properties bin/src/log.properties 2>/dev/null || true

BIN_SIZE=$(du -sh bin | awk '{print $1}')
echo "✓ Compilation successful. Size: $BIN_SIZE"
echo ""

# Step 3: Zip and upload to S3
echo "=== Step 3: Zipping and Uploading to S3 ==="
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
echo "✓ Uploaded. URL: $S3_URL"
echo ""

# Step 4: Deploy to KVS Coordinator instance
echo "=== Step 4: Deploying to KVS Coordinator Instance ==="
echo "Host: $KVS_COORD_HOST"
echo ""

# Download and extract bin from S3
echo "Downloading and extracting bin from S3..."
ssh $SSH_OPTS ec2-user@"$KVS_COORD_HOST" "mkdir -p $REMOTE_BASE_DIR && rm -rf $REMOTE_BIN_DIR && mkdir -p $REMOTE_LOGS_DIR && mkdir -p $REMOTE_BIN_DIR && cd $REMOTE_BASE_DIR && curl -s -o /tmp/bin.zip '$S3_URL' && unzip -q /tmp/bin.zip -d $REMOTE_BIN_DIR && rm /tmp/bin.zip" 2>/dev/null || true
echo "✓ Bin downloaded and extracted"

# Create libs directory and upload JAR files
echo "Uploading JAR dependencies..."
ssh $SSH_OPTS ec2-user@"$KVS_COORD_HOST" "mkdir -p $REMOTE_LIBS_DIR" 2>/dev/null || true
scp $SSH_OPTS "$JSOUP_JAR" ec2-user@"$KVS_COORD_HOST:$REMOTE_LIBS_DIR/" 2>/dev/null || true
scp $SSH_OPTS "$SNOWBALL_JAR" ec2-user@"$KVS_COORD_HOST:$REMOTE_LIBS_DIR/" 2>/dev/null || true
echo "✓ JAR files uploaded"

# Copy log.properties
ssh $SSH_OPTS ec2-user@"$KVS_COORD_HOST" "mkdir -p $REMOTE_SRC_DIR && cp $REMOTE_BIN_DIR/src/log.properties $REMOTE_SRC_DIR/log.properties 2>/dev/null || cp $REMOTE_BIN_DIR/log.properties $REMOTE_SRC_DIR/log.properties 2>/dev/null || true" 2>/dev/null || true

# Kill existing SearchServer process
echo "Checking for existing SearchServer process..."
BEFORE_KILL=$(ssh $SSH_OPTS ec2-user@"$KVS_COORD_HOST" "pgrep -f 'cis5550.jobs.SearchServer' | wc -l" 2>/dev/null || echo "0")
echo "SearchServer processes before killing: $BEFORE_KILL"

if [ "$BEFORE_KILL" != "0" ]; then
    echo "Killing existing SearchServer processes..."
    PIDS=$(ssh $SSH_OPTS ec2-user@"$KVS_COORD_HOST" "pgrep -f 'cis5550.jobs.SearchServer'" 2>/dev/null | tr '\n' ' ' || echo "")
    ssh $SSH_OPTS ec2-user@"$KVS_COORD_HOST" "for pid in $PIDS; do sudo kill \$pid 2>/dev/null || true; done; sleep 2; for pid in $PIDS; do sudo kill -9 \$pid 2>/dev/null || true; done" 2>/dev/null || true
    AFTER_KILL=$(ssh $SSH_OPTS ec2-user@"$KVS_COORD_HOST" "pgrep -f 'cis5550.jobs.SearchServer' | wc -l" 2>/dev/null || echo "0")
    echo "SearchServer processes after killing: $AFTER_KILL"
fi

# Check if port 10000 is in use
echo "Checking if port $SEARCH_PORT is available..."
PORT_IN_USE=$(ssh $SSH_OPTS ec2-user@"$KVS_COORD_HOST" "sudo lsof -i :$SEARCH_PORT -sTCP:LISTEN 2>/dev/null | wc -l" 2>/dev/null || echo "0")
if [ "$PORT_IN_USE" != "0" ]; then
    echo "WARNING: Port $SEARCH_PORT is already in use"
    echo "Killing process using port $SEARCH_PORT..."
    ssh $SSH_OPTS ec2-user@"$KVS_COORD_HOST" "sudo lsof -ti :$SEARCH_PORT | xargs sudo kill -9 2>/dev/null || true" 2>/dev/null || true
    sleep 2
fi

# Start SearchServer
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
echo "Starting SearchServer on port $SEARCH_PORT (as root)..."
echo "Logs will be written to: $REMOTE_LOGS_DIR/search-server-${TIMESTAMP}.log"

ssh -f $SSH_OPTS ec2-user@"$KVS_COORD_HOST" "cd $REMOTE_BASE_DIR && sudo nohup sh -c '/usr/bin/java -cp $REMOTE_BIN_DIR:$REMOTE_LIBS_DIR/jsoup-1.21.2.jar:$REMOTE_LIBS_DIR/libstemmer_java-3.0.1.jar cis5550.jobs.SearchServer $SEARCH_PORT localhost:$KVS_COORD_PORT' >$REMOTE_LOGS_DIR/search-server-${TIMESTAMP}.log 2>&1 &" 2>/dev/null || true

sleep 3

# Verify SearchServer is running
AFTER_START=$(ssh $SSH_OPTS ec2-user@"$KVS_COORD_HOST" "pgrep -f 'cis5550.jobs.SearchServer' | wc -l" 2>/dev/null || echo "0")
echo "SearchServer processes after starting: $AFTER_START"

if [ "$AFTER_START" != "0" ]; then
    echo "✓ SearchServer started successfully"
    echo ""
    echo "=== Deployment Complete ==="
    echo "Search server is running on: http://$KVS_COORD_HOST:$SEARCH_PORT"
    echo "Logs: $REMOTE_LOGS_DIR/search-server-${TIMESTAMP}.log"
    echo ""
    echo "To view logs:"
    echo "  ssh -i $SSH_KEY ec2-user@$KVS_COORD_HOST 'tail -f $REMOTE_LOGS_DIR/search-server-${TIMESTAMP}.log'"
    echo ""
    echo "To stop the server:"
    echo "  ssh -i $SSH_KEY ec2-user@$KVS_COORD_HOST 'sudo pkill -f cis5550.jobs.SearchServer'"
else
    echo "⚠ WARNING: SearchServer may not have started correctly"
    echo "Check logs: ssh -i $SSH_KEY ec2-user@$KVS_COORD_HOST 'tail -100 $REMOTE_LOGS_DIR/search-server-${TIMESTAMP}.log'"
fi

# Clean up local zip file
echo ""
echo "Cleaning up local zip file..."
rm -f "$ZIP_FILE"
echo "✓ Done"

