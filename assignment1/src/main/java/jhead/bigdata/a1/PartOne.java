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

public class PartOne {

    private static Configuration conf;
    private static FileSystem fs;
    private static String baseDirectory;

    private static String[] REMOTE_FILES = {
        "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/20417.txt.bz2",
        "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/5000-8.txt.bz2",
        "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/132.txt.bz2",
        "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/1661-8.txt.bz2",
        "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/972.txt.bz2",
        "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/19699.txt.bz2"
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

        // Download and decompress files
        for (final String dlFile : REMOTE_FILES) {
            System.out.println("Downloading: " + dlFile);
            final Path localPath = downloadFile(dlFile);

            System.out.println("Decompressing: " + localPath);
            decompressFile(localPath);

            System.out.println();
        }
    }

    private static Path downloadFile(final String uri)
        throws IOException {
        final URL remoteUrl = new URL(uri);
        final String filename = FilenameUtils.getName(remoteUrl.getPath());
        final Path localPath = new Path(filename);

        try (final InputStream in = remoteUrl.openStream();
             final OutputStream out = fs.create(localPath)) {
            IOUtils.copyBytes(in, out, conf);
        }

        // Return filename of downloaded file
        return localPath;
    }

    private static void decompressFile(final Path inputPath)
        throws IOException {
        final CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        final CompressionCodec codec = factory.getCodec(inputPath);

        if (codec == null) {
            System.err.println("No codec found for " + inputPath);
            System.exit(1);
        }

        final String outputUri =
                CompressionCodecFactory.removeSuffix(inputPath.toString(), codec.getDefaultExtension());
        final Path localPath = new Path(outputUri);

        try (final InputStream in = codec.createInputStream(fs.open(inputPath));
             final OutputStream out = fs.create(localPath)) {
            IOUtils.copyBytes(in, out, conf);
        }

        System.out.println("Decompressed to: " + localPath);
    }

}
