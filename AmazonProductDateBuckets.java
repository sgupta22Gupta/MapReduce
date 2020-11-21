import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

/***
 * This Map-Reduce code will go through every files exist in "hdfs input path" directory
 */
public class AmazonProductDateBuckets extends Configured implements Tool {
    // Just used for logging
    protected static final Logger LOG = LoggerFactory.getLogger(AmazonProductDateBuckets.class);

    // This is the execution entry point for Java programs
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new AmazonProductDateBuckets(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Need 2 argument (hdfs input path & hdfs output path), got: " + args.length);
            return -1;
        }

        // Now we create and configure a map-reduce "job"
        Job job = Job.getInstance(getConf(), "AmazonProductDateBuckets");
        job.setJarByClass(AmazonProductDateBuckets.class);


        // Specifies the Mapper class
        job.setMapperClass(MapReduceMapper.class);
        //job.setReducerClass(MapReduceReducer.class); //Not required

        // Specifies input and output directory paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  // The first argument must be an output path

        // Specifies the output types and formator
        //job.setMapOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(CompleteCombineFileInputFormat.class);
        job.setNumReduceTasks(1);

        // What for the job to complete and exit with appropriate exit code
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class MapReduceMapper extends Mapper<NullWritable, Text, Text, Text> {
        private static final Logger LOG = LoggerFactory.getLogger(MapReduceMapper.class);
        public static final Pattern NEW_LINE_CHARS = Pattern.compile("[\r\n]");
        public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("MMMM dd, yyyy");
        public static final SimpleDateFormat MONTH_NAME_FORMAT = new SimpleDateFormat("MMMM");

        private final static Text bucket = new Text();

        private JsonParser parser;        // This gson parser will help us parse JSON

        // This setup method is called once before the task is started
        @Override
        protected void setup(Context context) {
            parser = new JsonParser();
        }

        // This "map" method is called with every input file content as value.
        @Override
        public void map(NullWritable rowKey, Text value, Context context) throws InterruptedException, IOException {
            try {
                // Here we get the json data (stored as a string) from the appropriate column
                String jsonString = value.toString();

                // Now we parse the string into a JsonElement so we can dig into it
                JsonElement jsonTree = parser.parse(jsonString);

                final JsonObject fieldElements = jsonTree.getAsJsonObject();

                String year = "Bad";
                String month = "Bad";
                String date = "Bad";
                String mainCat = "";
                String title = "";

                if (fieldElements.has("date")) {
                    final JsonElement dateEl = fieldElements.get("date");
                    if (!dateEl.isJsonNull()) {
                        try {
                            final Date dt = DATE_FORMAT.parse(dateEl.getAsString());

                            year = String.valueOf(dt.getYear() + 1900);
                            month = MONTH_NAME_FORMAT.format(dt);
                            date = String.valueOf(dt.getDate());

                        } catch (ParseException e) {
                            LOG.warn("Failed to parse date field: " + e.getMessage());
                        }
                    }
                }
                if (fieldElements.has("main_cat")) {
                    mainCat = fieldElements.get("main_cat").getAsString();
                }
                if (fieldElements.has("title")) {
                    title = fieldElements.get("title").getAsString();
                }

                // Write final output
                bucket.set(year + "\t" + month + "\t" + date);
                value.set(mainCat + "\t" + title);
                context.write(bucket, value);

            } catch (Exception e) {
                LOG.error("Error in MAP process: " + e.getMessage(), e);
            }
        }
    }

    // Reducer to simply combine the output
    public static class MapReduceReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
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

