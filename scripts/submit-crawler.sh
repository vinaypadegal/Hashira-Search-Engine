#!/bin/bash
# Submit Crawler Job to Running Flame Services
# Usage: ./submit-crawler.sh <seedUrl|comma-separated-urls> [groupSize] [blacklistTable]
# Example: ./submit-crawler.sh "https://example.com"
# Example: ./submit-crawler.sh "https://url1.com,https://url2.com,https://url3.com" 2
# Example: ./submit-crawler.sh "https://example.com" 1 "blacklist"
#
# Prerequisites: KVS and Flame services must be running
# Use ./start-services.sh to start them first

set -e

# Get the project root directory (parent of scripts/)
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

# Parse arguments
seedInput=${1:-""}
arg2=${2:-""}
arg3=${3:-""}

if [ -z "$seedInput" ]; then
    echo "Error: Seed URL(s) required"
    echo "Usage: ./submit-crawler.sh <seedUrl|comma-separated-urls> [groupSize] [blacklistTable]"
    echo "  groupSize: Number of URLs per job (default: 1)"
    echo "Example: ./submit-crawler.sh \"https://example.com\""
    echo "Example: ./submit-crawler.sh \"https://url1.com,https://url2.com\" 2"
    echo ""
    echo "Note: Make sure KVS and Flame services are running first!"
    echo "      Use ./start-services.sh to start them."
    exit 1
fi

# Parse groupSize and blacklistTable
groupSize=1
blacklistTable=""

if [ -n "$arg2" ]; then
  if [[ "$arg2" =~ ^[0-9]+$ ]]; then
    groupSize=$arg2
    blacklistTable="$arg3"
  else
    blacklistTable="$arg2"
  fi
fi

# Parse seed URLs (comma-separated or single)
seedUrls=()
if [[ "$seedInput" == *","* ]]; then
  IFS=',' read -ra URL_ARRAY <<< "$seedInput"
  for url in "${URL_ARRAY[@]}"; do
    url=$(echo "$url" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
    if [ -n "$url" ]; then
      seedUrls+=("$url")
    fi
  done
else
  seedUrls=("$seedInput")
fi

# Check if services are running
if ! curl -s http://localhost:9000/ > /dev/null 2>&1; then
    echo "Error: Flame Coordinator is not responding on port 9000"
    echo "Please start services first using: ./start-services.sh"
    exit 1
fi

if ! curl -s http://localhost:8000/ > /dev/null 2>&1; then
    echo "Error: KVS Coordinator is not responding on port 8000"
    echo "Please start services first using: ./start-services.sh"
    exit 1
fi

echo "==================================="
echo "Submitting Crawler Job"
echo "==================================="
echo "Total URLs: ${#seedUrls[@]}"
echo "Group size: $groupSize (URLs per job)"
if [ -n "$blacklistTable" ]; then
    echo "Blacklist Table: $blacklistTable"
fi
echo ""

# Create bin directory if it doesn't exist
mkdir -p bin

# Compile source files if needed
if [ ! -d "bin/cis5550" ] || [ "src/cis5550/jobs/Crawler.java" -nt "bin/cis5550/jobs/Crawler.class" ]; then
    echo "Compiling source files to bin/..."
    javac --release 21 -d bin src/cis5550/**/*.java
fi

# Create JAR file for Crawler
echo "Creating JAR file for Crawler..."
mkdir -p jars
jarFile="$PROJECT_ROOT/jars/crawler.jar"

# Create a temporary manifest file
manifestFile=$(mktemp)
echo "Manifest-Version: 1.0" > "$manifestFile"
echo "Main-Class: cis5550.jobs.Crawler" >> "$manifestFile"

# Build JAR file with all classes (needed for proper execution on workers)
cd bin
echo "Including all classes in JAR for proper execution..."
jar cfm "$jarFile" "$manifestFile" cis5550/ 2>/dev/null || {
    echo "Error: Failed to create JAR file"
    cd "$PROJECT_ROOT"
    rm -f "$manifestFile"
    exit 1
}
cd "$PROJECT_ROOT"
rm -f "$manifestFile"

if [ ! -f "$jarFile" ]; then
    echo "Error: Failed to create JAR file"
    exit 1
fi

echo "JAR file created: $jarFile"
echo ""

# Group URLs and submit crawler jobs
echo "Submitting crawler job(s) to Flame Coordinator..."

jobNum=1
totalJobs=$(( (${#seedUrls[@]} + groupSize - 1) / groupSize ))
successCount=0
failCount=0

for ((i=0; i<${#seedUrls[@]}; i+=groupSize)); do
  # Get URLs for this group
  groupUrls=()
  for ((j=i; j<i+groupSize && j<${#seedUrls[@]}; j++)); do
    groupUrls+=("${seedUrls[j]}")
  done
  
  # Join URLs with comma
  urlGroup=$(IFS=','; echo "${groupUrls[*]}")
  
  echo "Submitting job $jobNum/$totalJobs with ${#groupUrls[@]} URL(s)..."
  
  if [ -n "$blacklistTable" ]; then
    result=$(java -cp bin cis5550.flame.FlameSubmit localhost:9000 "$jarFile" cis5550.jobs.Crawler "$urlGroup" "$blacklistTable" 2>&1)
  else
    result=$(java -cp bin cis5550.flame.FlameSubmit localhost:9000 "$jarFile" cis5550.jobs.Crawler "$urlGroup" 2>&1)
  fi
  
  if echo "$result" | grep -q "OK"; then
    echo "  ✓ Job $jobNum submitted successfully"
    successCount=$((successCount + 1))
  else
    echo "  ✗ Job $jobNum failed: $result"
    failCount=$((failCount + 1))
  fi
  
  jobNum=$((jobNum + 1))
done

echo ""
echo "==================================="
echo "Submission Summary"
echo "==================================="
echo "Total jobs submitted: $((jobNum - 1))"
echo "Successful: $successCount"
echo "Failed: $failCount"
echo ""

if [ $successCount -gt 0 ]; then
    echo "The crawler is now running. You can:"
    echo "  - View services: tmux attach -t services"
    echo "  - Check KVS data: curl http://localhost:8000/view/pt-crawl"
    echo "  - Submit another job: ./submit-crawler.sh <url>"
fi

