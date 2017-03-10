package jhead.bigdata.a1;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class PartTwo {

    private static Configuration conf;
    private static FileSystem fs;
    private static String baseDirectory;

    private static String[] REMOTE_FILES = {
        "http://corpus.byu.edu/wikitext-samples/text.zip",
    };

    public static void main(final String[] args)
        throws IOException {
        if (args.length != 1) {
            System.err.println("Usage: hadoop jar ... <base-dir>");
            System.exit(1);
        }

        baseDirectory = args[0];
        System.out.println("HDFS base directory: " + baseDirectory);

        conf = new Configuration();
        fs = FileSystem.get(URI.create(baseDirectory), conf);

        System.out.println("Configuration loaded");
        System.out.println();

        // Download and decompress files
        for (final String dlFile : REMOTE_FILES) {
            System.out.println("Downloading zip file: " + dlFile);
            downloadFile(dlFile);

            System.out.println();
        }
    }

    private static void downloadFile(final String uri)
        throws IOException {
        final URL remoteUrl = new URL(uri);

        try (final ZipInputStream in = new ZipInputStream(remoteUrl.openStream())) {
            while (in.available() > 0) {
                final ZipEntry entry = in.getNextEntry();
                final String filename = FilenameUtils.getName(entry.getName());
                final Path localPath = new Path(filename);

                try (final OutputStream out = fs.create(localPath)) {
                    IOUtils.copyBytes(in, out, conf);
                }
            }
        }
    }

}
