import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * This Map-Reduce code will go through every files exist in "hdfs input path" directory
 */

public class AmazonReviewAnalyzeFields extends Configured implements Tool {

    // Just used for logging
    protected static final Logger LOG = LoggerFactory.getLogger(AmazonReviewAnalyzeFields.class);

    // This is the execution entry point for Java programs
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new AmazonReviewAnalyzeFields(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            LOG.error("Need 2 argument (hdfs input path & hdfs output path), got: " + args.length);
            System.err.println("Need 2 argument (hdfs input path & hdfs output path), got: " + args.length);
            return -1;
        }

        // Now we create and configure a map-reduce "job"
        Job job = Job.getInstance(getConf(), "AmazonReviewAnalyzeFields");
        job.setJarByClass(AmazonReviewAnalyzeFields.class);


        // Specifies the mapper & reducer class
        job.setMapperClass(MapReduceMapper.class);
        job.setReducerClass(MapReduceReducer.class);

        // Specifies input & output configurations
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  // The first argument must be an output path
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(CompleteCombineFileInputFormat.class);
        job.setNumReduceTasks(1);

        // What for the job to complete and exit with appropriate exit code
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class MapReduceMapper extends Mapper<NullWritable, Text, LongWritable, Text> {
        private static final Logger LOG = LoggerFactory.getLogger(AmazonReviewAnalyzeFields.MapReduceMapper.class);
        public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("MM dd, yyyy");

        private TreeMap<Long, String> topReviews;

        private JsonParser parser;        // This gson parser will help us parse JSON

        // This setup method is called once before the task is started
        @Override
        protected void setup(Context context) {
            topReviews = new TreeMap<>();
            parser = new JsonParser();

        }

        // This "map" method is called with every files to process
        @Override
        public void map(NullWritable rowKey, Text value, Context context) throws InterruptedException, IOException {
            try {
                // Here we get the json data (stored as a string) from the appropriate column
                String jsonString = value.toString();

                // Now we parse the string into a JsonElement so we can dig into it
                JsonElement jsonTree = parser.parse(jsonString);

                final JsonObject fieldElements = jsonTree.getAsJsonObject();

                if (fieldElements.has("reviewTime") && fieldElements.has("reviewerName") && fieldElements.has("overall")) {
                    final JsonElement dateEl = fieldElements.get("reviewTime");
                    if (!dateEl.isJsonNull()) {

                        final Date date = DATE_FORMAT.parse(dateEl.getAsString());

                        // Collect top 100 review
                        topReviews.put(date.getTime(), fieldElements.get("reviewerName").getAsString() + "\t" + fieldElements.get("overall").getAsString());

                        if (topReviews.size() > 2) {
                            topReviews.remove(topReviews.firstKey());
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error("Error in MAP process: " + e.getMessage(), e);
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            // Forward top 100 reviews to reducer
            LongWritable time = new LongWritable();
            Text value = new Text();
            for (Map.Entry<Long, String> entry : topReviews.entrySet()) {
                time.set(entry.getKey());
                value.set(entry.getValue());

                context.write(time, value);
            }
        }
    }

    // Reducer to simply combine the output
    public static class MapReduceReducer extends Reducer<LongWritable, Text, Text, Text> {

        private TreeMap<Long, String> topReviews;

        // This setup method is called once before the reducer task is started
        @Override
        protected void setup(Reducer.Context context) {
            topReviews = new TreeMap<>();
        }

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {

                // Combine top 100 reviews and keeps only 100 in memory
                topReviews.put(key.get(), value.toString());

                if (topReviews.size() > 2) {
                    topReviews.remove(topReviews.firstKey());
                }
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            Set<String> reviewers = new TreeSet<>();
            float overall = 0f;

            // Prepare set of reviewers and sum the review
            for (Map.Entry<Long, String> entry : topReviews.entrySet()) {
                final String[] data = entry.getValue().split("\t");
                if (data.length == 2) {
                    reviewers.add(data[0]);
                    overall += Float.parseFloat(data[1]);
                }
            }

            // Calculate average of review|Overall
            if (topReviews.size() > 0) {
                overall /= topReviews.size();
            }

            // Write final output
            context.write(new Text("Reviewer Names:"), new Text(String.join("\t", reviewers)));
            context.write(new Text("Average of Overall:"), new Text(String.valueOf(overall)));
        }
    }

    // CLASSES TO READ WHILE FILE AS SINGLE RECORD
    static class CompleteCombineFileInputFormat extends CombineFileInputFormat<NullWritable, Text> {
        public CompleteCombineFileInputFormat() {
        }

        public RecordReader<NullWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
            return new CombineFileRecordReader((CombineFileSplit) split, context, TextRecordReaderWrapper.class);
        }

        private static class TextRecordReaderWrapper extends CombineFileRecordReaderWrapper<NullWritable, Text> {
            public TextRecordReaderWrapper(CombineFileSplit split, TaskAttemptContext context, Integer idx) throws IOException, InterruptedException {
                super(new CompleteFileInputFormat(), split, context, idx);
            }
        }
    }

    static class CompleteFileInputFormat extends FileInputFormat<NullWritable, Text> {

        public CompleteFileInputFormat() {
        }

        @Override
        protected boolean isSplitable(JobContext context, Path file) {
            return false;
        }

        @Override
        public RecordReader<NullWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
                throws IOException, InterruptedException {
            CompleteFileRecordReader reader = new CompleteFileRecordReader();
            reader.initialize(split, context);
            return reader;
        }
    }

    static class CompleteFileRecordReader extends RecordReader<NullWritable, Text> {

        private FileSplit fileSplit;
        private Configuration conf;
        private Text value = new Text();
        private boolean processed = false;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            this.fileSplit = (FileSplit) split;
            this.conf = context.getConfiguration();
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (!processed) {
                byte[] contents = new byte[(int) fileSplit.getLength()];
                Path file = fileSplit.getPath();
                FileSystem fs = file.getFileSystem(conf);
                FSDataInputStream in = null;
                try {
                    in = fs.open(file);
                    IOUtils.readFully(in, contents, 0, contents.length);
                    value.set(contents, 0, contents.length);
                } finally {
                    IOUtils.closeStream(in);
                }
                processed = true;
                return true;
            }
            return false;
        }

        @Override
        public NullWritable getCurrentKey() throws IOException, InterruptedException {
            return NullWritable.get();
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException {
            return processed ? 1.0f : 0.0f;
        }

        @Override
        public void close() throws IOException {
// do nothing
        }

    }
}

