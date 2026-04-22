#!/bin/bash
# Start KVS, Flame Services, and Crawler
# Usage: ./start-all-with-crawler.sh [kvsWorkers] [flameWorkers] <seedUrl|seedFile|comma-separated-urls> [groupSize] [maxSeeds] [blacklistTable]
# Example: ./start-all-with-crawler.sh 2 3 "https://example.com" "blacklist"
# Example: ./start-all-with-crawler.sh 2 3 "https://url1.com,https://url2.com" 2
# Example: ./start-all-with-crawler.sh 2 3 seeds.txt 5 "blacklist"
# Example: ./start-all-with-crawler.sh 2 3 seeds.txt 5 10 "blacklist"  (read only first 10 seeds, group size 5)
# 
# If seedInput is a file, it will read all URLs from the file (or maxSeeds if specified)
# Lines starting with # are treated as comments
# groupSize: Number of URLs per job (default: 1)

set -e

# Get the project root directory (parent of scripts/)
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

# Parse arguments with defaults
kvsWorkers=${1:-1}
flameWorkers=${2:-2}
seedInput=${3:-""}
arg4=${4:-""}
arg5=${5:-""}
arg6=${6:-""}

# Parse groupSize, maxSeeds, and blacklistTable
groupSize=1
maxSeeds=0
blacklistTable=""

if [ -n "$arg4" ]; then
  if [[ "$arg4" =~ ^[0-9]+$ ]]; then
    # arg4 is a number - could be groupSize or maxSeeds
    if [ -n "$arg5" ] && [[ "$arg5" =~ ^[0-9]+$ ]]; then
      # arg4 is groupSize, arg5 is maxSeeds
      groupSize=$arg4
      maxSeeds=$arg5
      blacklistTable="$arg6"
    else
      # arg4 could be groupSize or maxSeeds - check context
      # If seedInput is a file, arg4 is likely maxSeeds; if comma-separated, it's groupSize
      if [[ "$seedInput" == *","* ]] || [ ! -f "$seedInput" ]; then
        groupSize=$arg4
        blacklistTable="$arg5"
      else
        maxSeeds=$arg4
        blacklistTable="$arg5"
      fi
    fi
  else
    # arg4 is not a number, so it's blacklistTable
    blacklistTable="$arg4"
  fi
fi

if [ -z "$seedInput" ]; then
    echo "Error: Seed URL or seed file is required"
    echo "Usage: ./start-all-with-crawler.sh [kvsWorkers] [flameWorkers] <seedUrl|seedFile|comma-separated-urls> [groupSize] [maxSeeds] [blacklistTable]"
    echo "  groupSize: Number of URLs per job (default: 1)"
    echo "Example: ./start-all-with-crawler.sh 2 3 \"https://example.com\" \"blacklist\""
    echo "Example: ./start-all-with-crawler.sh 2 3 \"https://url1.com,https://url2.com\" 2"
    echo "Example: ./start-all-with-crawler.sh 2 3 seeds.txt 5 \"blacklist\""
    echo ""
    echo "Seed file format: one URL per line, lines starting with # are comments"
    exit 1
fi

# Check if seedInput is a file
if [ -f "$seedInput" ] && [ -r "$seedInput" ]; then
    echo "Reading seed URLs from file: $seedInput"
    if [ $maxSeeds -gt 0 ]; then
        echo "Limiting to first $maxSeeds seed(s)"
    fi
    # Read URLs from file, skip empty lines and comments
    seedUrls=()
    while IFS= read -r line || [ -n "$line" ]; do
        # Stop if we've reached maxSeeds (if specified)
        if [ $maxSeeds -gt 0 ] && [ ${#seedUrls[@]} -ge $maxSeeds ]; then
            break
        fi
        # Trim whitespace
        line=$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
        # Skip empty lines and comments
        if [ -n "$line" ] && [ "${line#\#}" = "$line" ]; then
            seedUrls+=("$line")
        fi
    done < "$seedInput"
    
    if [ ${#seedUrls[@]} -eq 0 ]; then
        echo "Error: No valid URLs found in file $seedInput"
        exit 1
    fi
    
    echo "Found ${#seedUrls[@]} seed URL(s) in file"
    useFile=true
else
    # Check if it's comma-separated URLs or single URL
    if [[ "$seedInput" == *","* ]]; then
        IFS=',' read -ra URL_ARRAY <<< "$seedInput"
        seedUrls=()
        for url in "${URL_ARRAY[@]}"; do
            url=$(echo "$url" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            if [ -n "$url" ]; then
                seedUrls+=("$url")
            fi
        done
        echo "Found ${#seedUrls[@]} URL(s) in comma-separated input"
    else
        # Single URL
        seedUrls=("$seedInput")
    fi
    useFile=false
fi

echo "Group size: $groupSize (URLs per job)"

echo "==================================="
echo "Starting KVS, Flame Services, and Crawler"
echo "==================================="
echo "KVS Workers: $kvsWorkers"
echo "Flame Workers: $flameWorkers"
if [ "$useFile" = true ]; then
    echo "Seed URLs: ${#seedUrls[@]} URL(s) from $seedInput"
    echo "  URLs:"
    for url in "${seedUrls[@]}"; do
        echo "    - $url"
    done
else
    echo "Seed URL: $seedInput"
fi
if [ -n "$blacklistTable" ]; then
    echo "Blacklist Table: $blacklistTable"
fi
echo



# Create bin and logs directory if they don't exist
mkdir -p bin
mkdir -p logs

# JAR paths for compilation (matching start-services.sh)
JSOUP_JAR="$PROJECT_ROOT/libs/jsoup-1.21.2.jar"
SNOWBALL_JAR="$PROJECT_ROOT/libs/libstemmer_java-3.0.1.jar"

# Compile all source files to bin directory (with jsoup and snowball in classpath)
echo "Compiling source files to bin/ (with jsoup and snowball)..."
javac -cp "$JSOUP_JAR:$SNOWBALL_JAR" -d bin src/cis5550/**/*.java

# Create worker directories
for ((i=1; i<=kvsWorkers; i++)); do
    mkdir -p "worker$i"
done

if ! command -v tmux &> /dev/null; then
    echo "tmux is not installed. Install it with: brew install tmux (macOS) or sudo apt-get install tmux (Linux)"
    exit 1
fi

# Calculate total windows: 1 KVS-Coord + kvsWorkers + 1 Flame-Coord + flameWorkers + crawler windows
totalWindows=$((2 + kvsWorkers + flameWorkers))
# Calculate number of crawler jobs based on grouping
totalCrawlerJobs=$(( (${#seedUrls[@]} + groupSize - 1) / groupSize ))
totalWindows=$((totalWindows + totalCrawlerJobs))

echo
echo "Starting tmux session with $totalWindows windows..."
echo "Using tmux windows (not panes) for better organization"
echo

tmux kill-session -t services 2>/dev/null || true

# Create session with first window for KVS Coordinator
tmux new-session -d -s services -n "KVS-Coord"
tmux send-keys -t services:KVS-Coord "cd '$PROJECT_ROOT' && java -cp bin cis5550.kvs.Coordinator 8000 >> logs/kvs-coordinator.log 2>&1" C-m

# Create windows for KVS Workers and kill anything on the ports
for ((i=1; i<=kvsWorkers; i++)); do
    port=$((8000 + i))
    echo "[Worker $WORKER_NUM] Starting KVS Worker (no explicit ID)..."
    tmux new-window -t services -n "KVS-W$i"
    tmux send-keys -t services:KVS-W$i "cd '$PROJECT_ROOT' && java -cp bin cis5550.kvs.Worker $port worker$i localhost:8000 >> logs/kvs-worker-$i.log 2>&1" C-m
    sleep 0.5
done

# Create window for Flame Coordinator (needs jsoup + snowball for IndexerJsoup and PageRank)
tmux new-window -t services -n "Flame-Coord"
tmux send-keys -t services:Flame-Coord "cd '$PROJECT_ROOT' && java -cp 'bin:$JSOUP_JAR:$SNOWBALL_JAR' cis5550.flame.Coordinator 9000 localhost:8000 >> logs/flame-coordinator.log 2>&1" C-m
sleep 1

# Create windows for Flame Workers (needs jsoup + snowball for IndexerJsoup and PageRank)
for ((i=1; i<=flameWorkers; i++)); do
    port=$((9000 + i))
    tmux new-window -t services -n "Flame-W$i"
    # Use 'export' and semicolon to ensure persistence in the shell session
    tmux send-keys -t services:Flame-W$i "cd '$PROJECT_ROOT'; export FLAME_WORKER_ID=flame-worker-$i; java -cp 'bin:$JSOUP_JAR:$SNOWBALL_JAR' cis5550.flame.Worker $port localhost:9000 >> logs/flame-worker-$i.log 2>&1" C-m
    sleep 0.5
done

# Wait for services to be ready
echo "Waiting for services to start..."
sleep 3

# Wait for all Flame workers to register with the coordinator
echo "Waiting for all Flame workers to register..."
maxWaitTime=60  # Maximum wait time in seconds
waitInterval=1  # Check every second
elapsed=0
expectedWorkers=$flameWorkers

# First, wait a bit for coordinator to be ready
sleep 2

while [ $elapsed -lt $maxWaitTime ]; do
    # Try to get the number of registered workers
    workerResponse=$(curl -s "http://localhost:9000/workers" 2>/dev/null)
    
    if [ $? -eq 0 ] && [ -n "$workerResponse" ]; then
        registeredWorkers=$(echo "$workerResponse" | head -n 1 | tr -d '\n\r ' || echo "0")
        
        # Validate that we got a number
        if [[ "$registeredWorkers" =~ ^[0-9]+$ ]] && [ "$registeredWorkers" -ge "$expectedWorkers" ]; then
            echo "All $expectedWorkers Flame worker(s) registered!"
            break
        fi
        
        if [[ "$registeredWorkers" =~ ^[0-9]+$ ]]; then
            echo "  Registered: $registeredWorkers/$expectedWorkers workers (waiting...)"
        else
            echo "  Waiting for coordinator to be ready..."
        fi
    else
        echo "  Waiting for coordinator to be ready..."
    fi
    
    sleep $waitInterval
    elapsed=$((elapsed + waitInterval))
done

if [ $elapsed -ge $maxWaitTime ]; then
    echo "Warning: Timeout waiting for all workers to register. Proceeding anyway..."
    finalCount=$(curl -s "http://localhost:9000/workers" 2>/dev/null | head -n 1 | tr -d '\n\r ' || echo "unknown")
    echo "  Registered workers: $finalCount"
fi

# Create JAR file for Crawler
echo
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

# Group URLs and submit crawler jobs
echo
echo "Submitting crawler job(s)..."
totalJobs=$(( (${#seedUrls[@]} + groupSize - 1) / groupSize ))

if [ $totalJobs -eq 1 ]; then
    # Single job - use the Crawler window
    tmux new-window -t services -n "Crawler"
    sleep 2
    
    # Join all URLs with comma for this single job
    urlGroup=$(IFS=','; echo "${seedUrls[*]}")
    
    if [ -n "$blacklistTable" ]; then
        crawlerCmd="cd '$PROJECT_ROOT' && java -cp bin cis5550.flame.FlameSubmit localhost:9000 '$jarFile' cis5550.jobs.Crawler '$urlGroup' '$blacklistTable' >> logs/crawler.log 2>&1"
    else
        crawlerCmd="cd '$PROJECT_ROOT' && java -cp bin cis5550.flame.FlameSubmit localhost:9000 '$jarFile' cis5550.jobs.Crawler '$urlGroup' >> logs/crawler.log 2>&1"
    fi
    
    tmux send-keys -t services:Crawler "$crawlerCmd" C-m
    echo "  Submitted crawler job with ${#seedUrls[@]} URL(s)"
else
    # Multiple jobs - create a window for each group
    jobNum=1
    for ((i=0; i<${#seedUrls[@]}; i+=groupSize)); do
        # Get URLs for this group
        groupUrls=()
        for ((j=i; j<i+groupSize && j<${#seedUrls[@]}; j++)); do
            groupUrls+=("${seedUrls[j]}")
        done
        
        # Join URLs with comma
        urlGroup=$(IFS=','; echo "${groupUrls[*]}")
        
        windowName="Crawler-$jobNum"
        tmux new-window -t services -n "$windowName"
        sleep 1
        
        if [ -n "$blacklistTable" ]; then
            crawlerCmd="cd '$PROJECT_ROOT' && java -cp bin cis5550.flame.FlameSubmit localhost:9000 '$jarFile' cis5550.jobs.Crawler '$urlGroup' '$blacklistTable' >> logs/crawler-$jobNum.log 2>&1"
        else
            crawlerCmd="cd '$PROJECT_ROOT' && java -cp bin cis5550.flame.FlameSubmit localhost:9000 '$jarFile' cis5550.jobs.Crawler '$urlGroup' \"resume\" >> logs/crawler-$jobNum.log 2>&1"
        fi
        
        tmux send-keys -t services:"$windowName" "$crawlerCmd" C-m
        echo "  Submitted crawler job $jobNum/$totalJobs with ${#groupUrls[@]} URL(s)"
        jobNum=$((jobNum + 1))
    done
fi

echo
echo "All components started in tmux session 'services'"
echo
echo "Services running:"
echo "  - KVS Coordinator (port 8000)"
for ((i=1; i<=kvsWorkers; i++)); do
    echo "  - KVS Worker $i (port $((8000 + i)))"
done
echo "  - Flame Coordinator (port 9000)"
for ((i=1; i<=flameWorkers; i++)); do
    echo "  - Flame Worker $i (port $((9000 + i)))"
done
totalCrawlerJobs=$(( (${#seedUrls[@]} + groupSize - 1) / groupSize ))
if [ $totalCrawlerJobs -eq 1 ]; then
    echo "  - Crawler (submitted to Flame Coordinator with ${#seedUrls[@]} URL(s))"
else
    echo "  - $totalCrawlerJobs Crawler job(s) (submitted to Flame Coordinator, ${#seedUrls[@]} total URLs)"
fi
echo
echo "Tmux commands:"
echo "  - View: tmux attach -t services"
maxWindowNum=$((totalWindows - 1))
echo "  - Switch windows: Ctrl+B then window number (0-$maxWindowNum)"
echo "  - List windows: Ctrl+B then w"
echo "  - Next window: Ctrl+B then n"
echo "  - Previous window: Ctrl+B then p"
echo "  - Detach: Ctrl+B then D"
echo "  - Kill all: tmux kill-session -t services"
echo
totalCrawlerJobs=$(( (${#seedUrls[@]} + groupSize - 1) / groupSize ))
if [ $totalCrawlerJobs -eq 1 ]; then
    echo "Crawler is running in the 'Crawler' window. Check that window for output."
else
    echo "Crawler jobs are running in windows 'Crawler-1' through 'Crawler-$totalCrawlerJobs'. Check those windows for output."
fi
echo


