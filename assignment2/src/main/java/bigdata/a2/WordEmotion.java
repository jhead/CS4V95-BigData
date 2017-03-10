package bigdata.a2;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

public class WordEmotion {

    private static Set<String> positiveWords = new HashSet<>();
    private static Set<String> negativeWords = new HashSet<>();

    public static void load(final FileSystem fs)
        throws IOException {
        System.out.println("Loading word emotions");

        loadWordsFromFile(fs, "positive-words.txt", positiveWords);
        loadWordsFromFile(fs, "negative-words.txt", negativeWords);
    }

    private static void loadWordsFromFile(final FileSystem fs, final String file, final Set set)
        throws IOException {
        final Path path = new Path(file);

        // Automatically close file when done
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
            String word;

            // Read file line by line, one word per line, and add to set
            while ((word = reader.readLine()) != null) {
                set.add(word);
            }
        }
    }

    public static boolean isPositive(final String word) {
        return positiveWords.contains(word);
    }

    public static boolean isNegative(final String word) {
        return negativeWords.contains(word);
    }

}
