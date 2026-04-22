#!/bin/bash
# Hash a URL using BLAKE3 (via Hasher.hashFast)
# Usage: ./scripts/hash-url.sh <url>
# Example: ./scripts/hash-url.sh "https://en.wikipedia.org"

set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

if [ $# -lt 1 ]; then
    echo "Usage: $0 <url>"
    echo "Example: $0 \"https://en.wikipedia.org\""
    exit 1
fi

URL="$1"

# Compile if needed
if [ ! -f "bin/cis5550/tools/Hasher.class" ] || [ ! -f "bin/cis5550/tools/Blake3.class" ]; then
    echo "Compiling..."
    javac --release 21 -d bin src/cis5550/tools/*.java
fi

# Create temporary wrapper class
TMP_DIR=$(mktemp -d)
cat > "$TMP_DIR/HashURL.java" <<'EOF'
import cis5550.tools.Hasher;

public class HashURL {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: HashURL <url>");
            System.exit(1);
        }
        System.out.println(Hasher.hash(args[0]));
    }
}
EOF

javac --release 21 -cp bin -d "$TMP_DIR" "$TMP_DIR/HashURL.java"
java -cp "bin:$TMP_DIR" HashURL "$URL"
rm -rf "$TMP_DIR"

