#!/bin/bash
# Submit crawler job(s) to AWS Flame Coordinator
# Usage: ./scripts/submit-crawler-aws.sh <seedUrl|seedFile|comma-separated-urls> [groupSize] [maxSeeds] [blacklistTable]
# Examples:
#   ./scripts/submit-crawler-aws.sh "https://example.com"
#   ./scripts/submit-crawler-aws.sh "https://url1.com,https://url2.com,https://url3.com" 2
#   ./scripts/submit-crawler-aws.sh seeds.txt 5
#   ./scripts/submit-crawler-aws.sh seeds.txt 5 10 "blacklist-table"

set -euo pipefail

# Project root (parent of scripts/)
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

# --- Args & seed handling ---

seedInput=${1:-""}
arg2=${2:-""}
arg3=${3:-""}
arg4=${4:-""}

if [ -z "$seedInput" ]; then
  echo "Usage: $0 <seedUrl|seedFile|comma-separated-urls> [groupSize] [maxSeeds] [blacklistTable]"
  echo "  groupSize: Number of URLs per job (default: 1)"
  exit 1
fi

# Parse arguments: groupSize, maxSeeds, blacklistTable
groupSize=1
maxSeeds=0
blacklistTable=""

# Determine which argument is which
if [ -n "$arg2" ]; then
  if [[ "$arg2" =~ ^[0-9]+$ ]]; then
    groupSize=$arg2
    if [ -n "$arg3" ] && [[ "$arg3" =~ ^[0-9]+$ ]]; then
      maxSeeds=$arg3
      blacklistTable="$arg4"
    else
      blacklistTable="$arg3"
    fi
  else
    blacklistTable="$arg2"
  fi
fi

# Build list of seed URLs (file, comma-separated string, or single URL)
seedUrls=()

if [ -f "$seedInput" ] && [ -r "$seedInput" ]; then
  echo "Reading seed URLs from file: $seedInput"
  if [ $maxSeeds -gt 0 ]; then
    echo "Limiting to first $maxSeeds seed(s)"
  fi

  while IFS= read -r line || [ -n "$line" ]; do
    if [ $maxSeeds -gt 0 ] && [ ${#seedUrls[@]} -ge $maxSeeds ]; then
      break
    fi
    line=$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
    if [ -n "$line" ] && [ "${line#\#}" = "$line" ]; then
      seedUrls+=("$line")
    fi
  done < "$seedInput"

  if [ ${#seedUrls[@]} -eq 0 ]; then
    echo "Error: No valid URLs found in file $seedInput"
    exit 1
  fi
else
  # Check if it's comma-separated URLs
  if [[ "$seedInput" == *","* ]]; then
    IFS=',' read -ra URL_ARRAY <<< "$seedInput"
    for url in "${URL_ARRAY[@]}"; do
      url=$(echo "$url" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
      if [ -n "$url" ]; then
        seedUrls+=("$url")
      fi
    done
  else
    # Single URL
    seedUrls=("$seedInput")
  fi
fi

echo "Total URLs: ${#seedUrls[@]}"
echo "Group size: $groupSize (URLs per job)"
[ -n "$blacklistTable" ] && echo "Blacklist table: $blacklistTable"

# --- Get Flame Coordinator endpoint from Terraform ---

echo "Fetching Flame coordinator address from Terraform..."

if [ -d "terraform" ]; then
  FLAME_HOST=$(cd terraform && terraform output -raw flame_coordinator_public_dns 2>/dev/null || true)
  if [ -z "$FLAME_HOST" ]; then
    FLAME_HOST=$(cd terraform && terraform output -raw flame_coordinator_public_ip 2>/dev/null || true)
  fi
else
  FLAME_HOST=""
fi

if [ -z "$FLAME_HOST" ]; then
  echo "Error: Could not get flame_coordinator_public_dns or public_ip from Terraform."
  echo "Make sure 'terraform apply' has been run and outputs are available."
  exit 1
fi

FLAME_ENDPOINT="${FLAME_HOST}:9000"
echo "Using Flame coordinator at: $FLAME_ENDPOINT"

# --- Build crawler.jar locally ---

# JAR paths for compilation (matching start-services.sh)
JSOUP_JAR="$PROJECT_ROOT/libs/jsoup-1.21.2.jar"
SNOWBALL_JAR="$PROJECT_ROOT/libs/libstemmer_java-3.0.1.jar"

echo "Compiling sources (with jsoup and snowball)..."
mkdir -p bin
# Force clean compile - remove old class files to ensure fresh build
find bin -name "*.class" -delete 2>/dev/null || true
javac --release 21 -cp "$JSOUP_JAR:$SNOWBALL_JAR" -d bin src/cis5550/**/*.java

echo "Building crawler.jar..."
mkdir -p jars
JAR_FILE="$PROJECT_ROOT/jars/crawler.jar"
# Ensure absolute path
JAR_FILE="$(cd "$(dirname "$JAR_FILE")" && pwd)/$(basename "$JAR_FILE")"
# Remove old JAR to ensure fresh build
rm -f "$JAR_FILE"
MANIFEST_FILE=$(mktemp)
printf 'Manifest-Version: 1.0\nMain-Class: cis5550.jobs.Crawler\n' > "$MANIFEST_FILE"

jar cfm "$JAR_FILE" "$MANIFEST_FILE" -C bin cis5550 >/dev/null 2>&1
rm -f "$MANIFEST_FILE"

if [ ! -f "$JAR_FILE" ]; then
  echo "Error: Failed to create crawler.jar"
  exit 1
fi

JAR_SIZE=$(stat -f%z "$JAR_FILE" 2>/dev/null || stat -c%s "$JAR_FILE" 2>/dev/null || echo "unknown")
echo "JAR file created: $JAR_FILE (size: $JAR_SIZE bytes)"

# --- Group URLs and submit Flame jobs ---

echo
echo "Submitting crawler job(s) to $FLAME_ENDPOINT ..."

# Group URLs into groups of groupSize
jobNum=1
totalJobs=$(( (${#seedUrls[@]} + groupSize - 1) / groupSize ))
echo "Will submit $totalJobs job(s) with up to $groupSize URL(s) each"
echo

for ((i=0; i<${#seedUrls[@]}; i+=groupSize)); do
  # Get URLs for this group
  groupUrls=()
  for ((j=i; j<i+groupSize && j<${#seedUrls[@]}; j++)); do
    groupUrls+=("${seedUrls[j]}")
  done
  
  # Join URLs with comma
  urlGroup=$(IFS=','; echo "${groupUrls[*]}")
  
  echo "Submitting job $jobNum/$totalJobs with ${#groupUrls[@]} URL(s): $urlGroup"
  echo "  Using JAR: $JAR_FILE ($(stat -f%z "$JAR_FILE" 2>/dev/null || stat -c%s "$JAR_FILE" 2>/dev/null || echo "unknown") bytes)"
  
  if [ -n "$blacklistTable" ]; then
    nohup java -cp bin cis5550.flame.FlameSubmit \
      "$FLAME_ENDPOINT" \
      "$JAR_FILE" \
      cis5550.jobs.Crawler \
      "$urlGroup" \
      "$blacklistTable" \
      >/tmp/crawler-"$(date +%s)"-"$jobNum".log 2>&1 &
  else
    nohup java -cp bin cis5550.flame.FlameSubmit \
      "$FLAME_ENDPOINT" \
      "$JAR_FILE" \
      cis5550.jobs.Crawler \
      "$urlGroup" \
      >/tmp/crawler-"$(date +%s)"-"$jobNum".log 2>&1 &
  fi
  
  jobNum=$((jobNum + 1))
  # 100ms delay before submitting next job
  sleep 1
done

echo
echo "Done. Submitted $((jobNum - 1)) job(s)."


