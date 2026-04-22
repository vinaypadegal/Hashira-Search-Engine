package cis5550.test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Utility to generate test data files for KVS benchmarking.
 * Creates a directory with files of various sizes to test putRow/getRow performance.
 */
public class GenerateTestData {
    
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: GenerateTestData <output_directory> [num_files] [min_size_kb] [max_size_kb]");
            System.err.println("Example: GenerateTestData ./test-data 100 1 100");
            System.exit(1);
        }
        
        String outputDir = args[0];
        int numFiles = args.length > 1 ? Integer.parseInt(args[1]) : 100;
        int minSizeKB = args.length > 2 ? Integer.parseInt(args[2]) : 1;
        int maxSizeKB = args.length > 3 ? Integer.parseInt(args[3]) : 100;
        
        try {
            Path dir = Paths.get(outputDir);
            if (Files.exists(dir)) {
                System.out.println("Directory exists, cleaning...");
                deleteDirectory(dir.toFile());
            }
            Files.createDirectories(dir);
            
            System.out.println("Generating " + numFiles + " test files in " + outputDir);
            System.out.println("File sizes: " + minSizeKB + " KB to " + maxSizeKB + " KB");
            
            // Sample text to fill files
            String sampleText = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. " +
                    "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. " +
                    "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris. " +
                    "Duis aute irure dolor in reprehenderit in voluptate velit esse. " +
                    "Excepteur sint occaecat cupidatat non proident, sunt in culpa. " +
                    "The quick brown fox jumps over the lazy dog. " +
                    "0123456789 ABCDEFGHIJKLMNOPQRSTUVWXYZ abcdefghijklmnopqrstuvwxyz. " +
                    "This is test data for KVS benchmarking purposes. " +
                    "Performance testing requires realistic data patterns. " +
                    "Key-value stores need efficient read and write operations.\n";
            
            int bytesPerKB = 1024;
            int sampleBytes = sampleText.getBytes().length;
            
            for (int i = 0; i < numFiles; i++) {
                // Generate file size between min and max
                int sizeKB = minSizeKB + (int)(Math.random() * (maxSizeKB - minSizeKB + 1));
                int targetBytes = sizeKB * bytesPerKB;
                
                String fileName = String.format("test_file_%05d.txt", i);
                File file = dir.resolve(fileName).toFile();
                
                try (FileWriter writer = new FileWriter(file)) {
                    int written = 0;
                    while (written < targetBytes) {
                        int remaining = targetBytes - written;
                        if (remaining >= sampleBytes) {
                            writer.write(sampleText);
                            written += sampleBytes;
                        } else {
                            writer.write(sampleText.substring(0, remaining));
                            written += remaining;
                        }
                    }
                }
                
                if ((i + 1) % 10 == 0) {
                    System.out.println("Generated " + (i + 1) + " files...");
                }
            }
            
            long totalSize = Files.list(dir)
                    .filter(Files::isRegularFile)
                    .mapToLong(p -> {
                        try {
                            return Files.size(p);
                        } catch (IOException e) {
                            return 0;
                        }
                    })
                    .sum();
            
            System.out.println("\nDone! Generated " + numFiles + " files");
            System.out.println("Total size: " + (totalSize / 1024) + " KB (" + (totalSize / (1024 * 1024)) + " MB)");
            System.out.println("Directory: " + dir.toAbsolutePath());
            
        } catch (Exception e) {
            System.err.println("Error generating test data: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    private static void deleteDirectory(File dir) {
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    deleteDirectory(file);
                }
            }
        }
        dir.delete();
    }
}

