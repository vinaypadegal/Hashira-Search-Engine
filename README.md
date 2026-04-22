# CIS5550 Project Hashira


## Terraform quick start

- **Install AWS CLI** and configure credentials (`aws configure`) using access keys from the AWS console.
- **Install Terraform** (v1.5+), then from the `terraform/` directory run `terraform init`, `terraform plan`, and `terraform apply` to provision or update the cluster.
- **Most important**: after apply, run `terraform output` in the `terraform/` directory to get the coordinator/worker public IPs and DNS names you’ll use for SSH and debugging.

----

Shortcut to sign in to AWS: https://upenn-cis4550-cis4550f25009.signin.aws.amazon.com/console

## Commands run for HTTPS setup (Hashira.401.cis5550.net)

Chronological commands executed on the EC2 host:

```bash
sudo dnf install -y java augeas-libs
sudo python3 -m venv /opt/certbot
sudo /opt/certbot/bin/pip install --upgrade pip
sudo /opt/certbot/bin/pip install certbot
sudo /opt/certbot/bin/certbot certonly --standalone -d Hashira.401.cis5550.net

# Convert Let's Encrypt cert to PKCS12/JKS with password 000000
sudo openssl pkcs12 \
  -export \
  -in /etc/letsencrypt/live/hashira.401.cis5550.net/fullchain.pem \
  -inkey /etc/letsencrypt/live/hashira.401.cis5550.net/privkey.pem \
  -out /home/ec2-user/keystore.p12 \
  -name hashira.401.cis5550.net \
  -CAfile /etc/letsencrypt/live/hashira.401.cis5550.net/fullchain.pem \
  -caname "Let's Encrypt Authority X3" \
  -password pass:000000

sudo chmod 644 /home/ec2-user/keystore.p12

keytool -importkeystore \
  -deststorepass 000000 \
  -destkeypass 000000 \
  -deststoretype pkcs12 \
  -srckeystore /home/ec2-user/keystore.p12 \
  -srcstoretype PKCS12 \
  -srcstorepass 000000 \
  -alias hashira.401.cis5550.net \
  -destkeystore /home/ec2-user/keystore.jks
```

## SSH and file copy

- **SSH into an EC2 instance** (example):
  ```bash
  ssh -i "~/Downloads/amanMBP.pem" ec2-user@3.231.223.169
  ```
  Keep the `.pem` file that was shared over WhatsApp saved in your `Downloads` folder as `amanMBP.pem`.

- **Copy the project directory to an EC2 instance**:
  ```bash
  scp -r -i ~/Downloads/amanMBP.pem /local/path/to/directory ec2-user@<EC2_PUBLIC_IP>:/home/ec2-user/
  ```

A distributed web crawler and data processing framework built with Java, featuring a Key-Value Store (KVS), Flame distributed computing framework, and web server components.

## Quick Start

### Prerequisites

- Java JDK 8 or higher
- `tmux` (for service management)
- `curl` (for testing)

### Start Services and Run Crawler

**Option 1: Start everything at once**

```bash
./scripts/start-all-with-crawler.sh [kvsWorkers] [flameWorkers] <seedUrl> [blacklistTable]
```

**Example:**

```bash
./scripts/start-all-with-crawler.sh 1 2 "https://en.wikipedia.org/wiki/Main_Page"
```

**Option 2: Start services separately**

```bash
# Start KVS and Flame services
./scripts/start-services.sh [kvsWorkers] [flameWorkers]

# Submit crawler job (in another terminal)
./scripts/submit-crawler.sh <seedUrl> [blacklistTable]
```

### Stop Services

```bash
./scripts/stop-services.sh
```

## Main Components

### 1. **Key-Value Store (KVS)**

- Distributed key-value storage system
- Coordinator on port 8000
- Workers on ports 8001, 8002, etc.
- View data: `curl http://localhost:8000/view/pt-crawl`

### 2. **Flame Framework**

- Distributed computing framework for parallel processing
- Coordinator on port 9000
- Workers on ports 9001, 9002, etc.

### 3. **Web Crawler**

- Respects robots.txt and crawl-delay
- Extracts and normalizes URLs
- Stores anchor text with HTML entity decoding
- Handles redirects and canonical URLs
- Stores crawled pages in `pt-crawl` table

### 4. **Web Server**

- RESTful HTTP server framework
- Session management
- Route handling with path parameters

### 5. **Search Engine**

- Full-text search with advanced ranking
- TF-IDF with cosine similarity
- Field-based ranking (title, meta, header, body, URL, anchor)
- Query term proximity scoring
- Phrase search support
- Debug mode for ranking analysis
- Web-based search interface

## Crawler Features

### Anchor Text Extraction

- Extracts anchor text from `<a href="...">text</a>` tags
- Decodes HTML entities (`&amp;`, `&copy;`, `&#39;`, etc.)
- Normalizes whitespace
- Stores in columns: `anchors:<sourceUrl>` → `<anchorText>`
- Multiple anchor texts from same source are concatenated

### URL Normalization

- Handles relative and absolute URLs
- Resolves protocol, host, port, path, query, fragment
- Deduplicates URLs

### Robots.txt Compliance

- Fetches and caches robots.txt per host
- Respects `Disallow` and `Allow` rules
- Implements `Crawl-Delay` with per-host rate limiting

## Project Structure

```
├── src/cis5550/          # Source code
│   ├── jobs/             # Crawler and job implementations
│   ├── kvs/              # Key-Value Store
│   ├── flame/            # Flame distributed computing
│   ├── webserver/        # Web server framework
│   └── tools/            # Utility classes
├── scripts/               # Shell scripts for running services
├── bin/                   # Compiled classes
└── jars/                  # JAR files for jobs
```

## Search Engine

### Prerequisites

Before using the search engine, you need to:

1. **Crawl pages** (see Crawler section above)
2. **Build the index** using the Indexer job:
   ```bash
   ./scripts/run-indexer.sh
   ```
   Or use Indexer3 for advanced features:
   ```bash
   # Submit Indexer3 job via Flame
   java -cp bin cis5550.flame.FlameSubmit localhost:9000 indexer.jar cis5550.jobs.Indexer3
   ```
3. **Compute PageRank** (optional but recommended for better ranking):

   ```bash
   # Use default values (threshold: 0.01, percentage: 100%)
   ./scripts/run-pagerank.sh

   # Custom convergence threshold
   ./scripts/run-pagerank.sh 0.001

   # Custom threshold and convergence percentage
   ./scripts/run-pagerank.sh 0.001 95
   ```

### Starting the Search Server

```bash
# Start search server on default port (8080)
./scripts/start-search-server.sh

# Start on custom port
./scripts/start-search-server.sh 3000

# Start with custom port and KVS coordinator
./scripts/start-search-server.sh 3000 localhost:8000
```

The search server will:

- Compile Java source files automatically
- Check if KVS coordinator is running
- Start on the specified port (default: 8080)
- Be accessible at `http://localhost:8080`

### Using the Search Interface

1. **Open your browser** and navigate to `http://localhost:8080`
2. **Enter your search query** in the search box
3. **Enable Debug Mode** (optional) to see detailed ranking information:
   - Term frequencies by field (title, meta, header, body, URL, anchor)
   - Individual ranking scores (cosine similarity, field score, proximity, phrase, URL/anchor)
   - PageRank values
4. **Click Search** to see results ranked by relevance

### PageRank Computation

PageRank is a link-based ranking algorithm that measures the importance of pages based on the structure of the web graph. It's computed iteratively until convergence.

**Running PageRank:**

```bash
# Default: threshold=0.01, percentage=100%
./scripts/run-pagerank.sh

# Custom convergence threshold (smaller = more precise)
./scripts/run-pagerank.sh 0.001

# Custom threshold and convergence percentage
./scripts/run-pagerank.sh 0.001 95
```

**Parameters:**

- **Threshold** (required): Maximum change in PageRank value for a page to be considered "converged" (default: 0.01)
- **Percentage** (optional): Percentage of pages that must converge before stopping (default: 100%)

**How it works:**

1. Loads all pages from `pt-crawl` table
2. Extracts outgoing links from each page
3. Iteratively computes PageRank using the formula: `PR(A) = (1-d) + d × Σ(PR(T)/C(T))`
   - `d` = decay factor (0.85)
   - `T` = pages linking to A
   - `C(T)` = number of outgoing links from T
4. Stops when convergence criteria are met
5. Saves results to `pt-pageranks` table

**Checking PageRank results:**

```bash
# View all PageRank values
curl http://localhost:8000/view/pt-pageranks

# Get PageRank for a specific URL (hash the URL first)
curl http://localhost:8000/get/pt-pageranks/{urlHash}/rank
```

**Note:** PageRank computation may take several minutes depending on the size of your crawl. The algorithm includes detailed logging to track progress.

### Search Features

- **TF-IDF with Cosine Similarity**: Measures document relevance using term frequency and inverse document frequency
- **Field-Based Ranking**: Different weights for title (2.0×), meta (1.2×), header (1.5×), body (1.0×), URL (1.8×), and anchor text (1.3×)
- **Query Term Proximity**: Documents where query terms appear close together score higher
- **Phrase Search**: Supports quoted phrases for exact phrase matching
- **URL and Anchor Text Matching**: Boosts pages where query terms appear in URLs or anchor text (off-page features)
- **PageRank Integration**: Combines content-based ranking with link-based PageRank scores

### Stopping the Search Server

```bash
# Stop server on default port (8080)
./scripts/stop-search-server.sh

# Stop server on custom port
./scripts/stop-search-server.sh 3000
```

Or manually:

```bash
# Find and kill process on port 8080
lsof -ti :8080 | xargs kill

# Or use pkill
pkill -f "cis5550.jobs.SearchServer"
```

### Debug Mode

When debug mode is enabled, each search result shows:

- **Term Frequencies**: How often each query term appears in different document fields
- **PageRank**: The PageRank value for the page
- **Component Scores**: Breakdown of all ranking components:
  - Cosine Similarity Score
  - Field-Based Score
  - Proximity Score
  - Phrase Score
  - URL/Anchor Score
- **Final Score**: The combined ranking score

This information helps understand why certain pages rank higher than others.

## Common Commands

```bash
# Check if services are running
./scripts/check-services.sh

# View KVS tables
curl http://localhost:8000/view/pt-crawl
curl http://localhost:8000/view/pt-index
curl http://localhost:8000/view/pt-pageranks

# View Flame coordinator
curl http://localhost:9000/

# Attach to tmux session (if services running in tmux)
tmux attach -t services

# Build search index
./scripts/run-indexer.sh

# Compute PageRank
./scripts/run-pagerank.sh

# Start search server
./scripts/start-search-server.sh

# Stop search server
./scripts/stop-search-server.sh
```

## Scaling with Multiple Instances (Terraform)

To scale crawling across multiple EC2 instances for faster scraping:

### 0. Set Up AWS Credentials (Required First!)

```bash
# Install AWS CLI (if needed)
brew install awscli  # macOS

# Configure credentials
aws configure
# Enter your AWS Access Key ID, Secret Access Key, and region

# Verify setup
./scripts/check-aws-credentials.sh
```

See `terraform/SETUP_CREDENTIALS.md` for detailed instructions.

### 1. Configure Terraform

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your settings (instance_count, key_name, etc.)
```

### 2. Deploy Infrastructure

```bash
cd terraform
terraform init
terraform plan
terraform apply
```

This creates multiple EC2 instances (default: 4) with:

- Security groups for KVS/Flame ports
- Java and tmux pre-installed
- IAM roles configured

### 3. Deploy Code to All Instances

```bash
./scripts/deploy-to-instances.sh
```

This compiles your code and copies it to all EC2 instances.

### 4. Start Distributed Crawling

Create a seed URLs file:

```bash
cat > seeds.txt << EOF
https://en.wikipedia.org/wiki/Main_Page
https://de.wikipedia.org/wiki/Hauptseite
https://fr.wikipedia.org/wiki/Accueil
https://es.wikipedia.org/wiki/Portada
EOF
```

Start crawling across all instances:

```bash
./scripts/start-distributed-crawl.sh seeds.txt 2 4
```

URLs are distributed round-robin across instances. Each instance runs its own KVS/Flame cluster.

### 5. Monitor Instances

```bash
./scripts/monitor-instances.sh
```

### 6. Clean Up

```bash
cd terraform
terraform destroy
```

### Scaling Strategy

- **Horizontal Scaling**: Each instance crawls different seed URLs independently
- **No Shared State**: Each instance has its own KVS (no coordination needed)
- **Parallel Processing**: Multiple instances = N× faster crawling
- **Resource Isolation**: One instance failure doesn't affect others

**Example**: 4 instances × 4 Flame workers each = 16 parallel crawl workers

## Notes

- Services run in background via tmux
- Crawler data is stored in KVS table `pt-crawl`
- Each crawled page has columns: `url`, `responseCode`, `contentType`, `page`, `anchors:<sourceUrl>`
- Ports: KVS Coordinator (8000), Flame Coordinator (9000), Workers (8001+, 9001+), Search Server (8080 by default)
- For distributed crawling, each instance maintains its own KVS (no shared state)
- Search index is stored in KVS table `pt-index` (created by Indexer job)
- PageRank values are stored in KVS table `pt-pageranks` (created by PageRank job)
- The search server requires the index to be built before searching
- PageRank computation is optional but recommended for better search ranking
