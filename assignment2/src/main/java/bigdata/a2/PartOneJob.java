package bigdata.a2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Hadoop MapReduce job that gets created and executed by our application
 *
 * Mapper breaks up a file into words and outputs if it's positive or negative
 * Reducer counts total positive/negative words
 */
public class PartOneJob {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // Load word mappings on each mapper
            WordEmotion.load(FileSystem.get(context.getConfiguration()));

            StringTokenizer itr = new StringTokenizer(value.toString());

            // Runs once for each word in file
            String word;
            while (itr.hasMoreTokens()) {
                word = itr.nextToken();

                // Check word emotion
                if (WordEmotion.isPositive(word)) {
                    context.write(new Text("positive"), one);
                } else if (WordEmotion.isNegative(word)) {
                    context.write(new Text("negative"), one);
                }

                // Skip if word is not recognized
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    public int run(final Configuration conf, final List<Path> inputs) throws Exception {
        Job job = Job.getInstance(conf, "cs4v95.assignment2");

        // Job config
        job.setJarByClass(PartOneJob.class);
        job.setNumReduceTasks(1);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Add all files to job input
        for (final Path inputPath : inputs) {
            FileInputFormat.addInputPath(job, inputPath);
        }

        // Set our output dir
        FileOutputFormat.setOutputPath(job, PartOne.OUTPUT_PATH);

        // Run and wait
        return job.waitForCompletion(true) ? 0 : 1;
    }

}
