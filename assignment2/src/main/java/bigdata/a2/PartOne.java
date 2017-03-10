package bigdata.a2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;

/**
 * Assignment 2 Part 1
 * Entry point
 *
 * Here we setup our Hadoop/HDFS config, FS, and environment, then create and run our word count job.
 */
public class PartOne {

    public static final Path OUTPUT_PATH = new Path("a2part1-output");

    private static Configuration conf;
    private static FileSystem fs;
    private static String baseDirectory;

    public static void main(final String[] args)
            throws Exception {
        // User ran program without any arguments
        if (args.length != 1) {
            System.err.println("Usage: hadoop jar ... <base-dir>");
            System.exit(1);
        }

        // Get HDFS base directory from command line args
        baseDirectory = args[0];
        System.out.println("HDFS base directory: " + baseDirectory);

        // Hadoop config
        conf = new Configuration();

        // Environment setup
        conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
        conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
        conf.set("mapreduce.framework.name", "yarn");

        // HDFS
        fs = FileSystem.get(URI.create(baseDirectory), conf);
        System.out.println("Output directory: " + OUTPUT_PATH.toString());

        // Clean up output dir before starting
        cleanup();

        // Get a list of text files from our base directory to pass to our job
        final List<Path> files = getFileList();

        // Create and run the job on the file list
        // run() returns an exit code depending on whether or not the job was successful
        new PartOneJob().run(conf, files);
    }

    /**
     * Gets a list of the files and folders in our base directory then filters them.
     * This should only return files that end with .txt
     *
     * @return List of file paths
     */
    public static List<Path> getFileList()
        throws IOException {
        System.out.println("Loading input file list");

        final FileStatus[] statusList = fs.listStatus(new Path("data/"), new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().endsWith(".txt");
            }
        });

        // Convert FileStatus array to a list of paths
        final List<Path> files = new LinkedList<>();
        for (final FileStatus status : statusList) {
            System.out.println("Input: " + status.getPath().toString());
            files.add(status.getPath());
        }

        return files;
    }

    /**
     * Cleans up job output directory before starting
     */
    private static void cleanup()
        throws IOException {
        if (fs.exists(OUTPUT_PATH)) {
            fs.delete(OUTPUT_PATH, true);
        }
    }

}
