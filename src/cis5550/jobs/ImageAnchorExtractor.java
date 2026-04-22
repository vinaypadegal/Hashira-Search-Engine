package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;

public class ImageAnchorExtractor {

    /**
     * Fetch all URLs from pt-crawl that end with .jpg or .png (case-insensitive),
     * collect their anchor texts, and write them to pt-image-crawl with:
     * - row key: url (as-is)
     * - column: page (space-separated anchor texts)
     */
    public static void run(FlameContext ctx, String[] args) throws Exception {
        // Step 1: pull relevant rows from pt-crawl
        FlameRDD images = ctx.fromTableParallel("pt-crawl", row -> {
            String url = row.get("url");
            if (url == null) {
                return null;
            }
            String lower = url.toLowerCase();
            if (!(lower.endsWith(".jpg") || lower.endsWith(".jpeg") || lower.endsWith(".png"))) {
                return null;
            }

            // Gather anchor texts from columns named anchors:*
            StringBuilder anchors = new StringBuilder();
            for (String col : row.columns()) {
                if (col.startsWith("anchors:")) {
                    String anchor = row.get(col);
                    if (anchor != null && !anchor.isEmpty()) {
                        if (anchors.length() > 0) {
                            anchors.append(" ");
                        }
                        anchors.append(anchor.trim());
                    }
                }
            }

            // keep even if no anchors (page column can be empty)
            return url + "\t" + anchors.toString();
        });

        // Step 2: map to pairs (url, anchors)
        // Step 2: convert to pairs (url, anchors) and save as table
        FlamePairRDD pairs = images.mapToPairParallel((str, kvs, writer) -> {
            int idx = str.indexOf('\t');
            if (idx < 0) {
                return null;
            }
            String url = str.substring(0, idx);
            String anchors = str.substring(idx + 1);
            return new FlamePair(url, anchors);
        });

        pairs.saveAsTable("pt-image-crawl"); // stored in column "value" (anchors)

        ctx.output("Completed writing image anchors to pt-image-crawl");
    }
}
