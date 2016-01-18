package ca.uwaterloo.cs.bigdata2016w.esnrRahman.assignment1;

import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import tl.lin.data.map.HMapStFW;
import tl.lin.data.pair.PairOfStrings;

public class StripesPMI extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(StripesPMI.class);
    private static final String SIDE_DATA_PATH = "sidedata";
    private static final String TOTAL_NUMBER_OF_LINES = "TOTAL_NUMBER_OF_LINES";

    // Mapper: emits (token, 1) for every word occurrence.
    // It also emits (*, 1) (where * is expressed as a token) for every line occurrence
    private static class FirstJobMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
        // Reuse objects to save overhead of object creation.
        private final static PairOfStrings PAIR = new PairOfStrings();
        private static final IntWritable ONE = new IntWritable(1);
        private int window = 2;

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = ((Text) value).toString();

            // Count # of lines
            PAIR.set("*", "*");
            context.write(PAIR, ONE);

            StringTokenizer itr = new StringTokenizer(line);

            int cnt = 0;
            Set set = Sets.newHashSet();
            while (itr.hasMoreTokens()) {
                cnt++;
                String w = itr.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
                if (w.length() == 0) continue;

                // Do a unique word count per line
                if (set.add(w)) {
                    PAIR.set(w, "*");
                    context.write(PAIR, ONE);
                }

                // Consider up to the first 100 words in each line
                if (cnt >= 100) break;
            }
        }
    }

    protected static class FirstJobCombiner extends
            Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
        private static final IntWritable SUM = new IntWritable();

        @Override
        public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> iter = values.iterator();
            while (iter.hasNext()) {
                sum += iter.next().get();
            }
            SUM.set(sum);
            context.write(key, SUM);
        }
    }

    private static class FirstJobReducer extends
            Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
        private final static IntWritable SUM = new IntWritable();

        @Override
        public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            Iterator<IntWritable> iter = values.iterator();
            int sum = 0;
            while (iter.hasNext()) {
                sum += iter.next().get();
            }

            SUM.set(sum);
            context.write(key, SUM);
        }
    }

    // Mapper: emits (pair, 1) for every co-occurrence pair
    private static class SecondJobMapper extends Mapper<LongWritable, Text, Text, HMapStFW> {
        // Reuse objects to save overhead of object creation.
        private static final Text KEY = new Text();
        private static final HMapStFW MAP = new HMapStFW();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = ((Text) value).toString();

            List<String> tokens = new ArrayList<String>();
            StringTokenizer itr = new StringTokenizer(line);

            int cnt = 0;

            ArrayList<String> set= new ArrayList<String>();

            while (itr.hasMoreTokens()) {
                cnt++;
                String w = itr.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
                if (w.length() == 0) continue;
                if(set.contains(w)) continue;
                tokens.add(w);
                set.add(w);

                // Consider up to the first 100 words in each line
                if (cnt >= 100) break;
            }

            // Find the co-occurring words
            for(int i = 0; i < set.size(); i++) {
                MAP.clear();
                String leftWord = set.get(i);
                for(int j = 0; j < set.size(); j++) {

                    String rightWord = set.get(j);
                    if (leftWord.equals(rightWord)) continue;
                    MAP.increment(set.get(j));

                }
                KEY.set(leftWord);
                context.write(KEY, MAP);
            }
        }
    }

    private static class SecondJobCombiner extends Reducer<Text, HMapStFW, Text, HMapStFW> {
        @Override
        public void reduce(Text key, Iterable<HMapStFW> values, Context context)
                throws IOException, InterruptedException {
            Iterator<HMapStFW> iter = values.iterator();

            HMapStFW map = new HMapStFW();

            while (iter.hasNext()) {
                map.plus(iter.next());
            }

            context.write(key, map);
        }
    }


    private static class SecondJobReducer extends
            Reducer<Text, HMapStFW, Text, HMapStFW> {
        private final static DoubleWritable PMI = new DoubleWritable();
        private static Map<String, Float> WordOccurrences = new HashMap<String, Float>();

        // NOTE: Took reference from the link below to read file from HDFS -
        // http://stackoverflow.com/questions/26209773/hadoop-map-reduce-read-a-text-file
        // This did not work, so asked a friend and he suggested SequenceFile
        @Override
        public void setup(Context context) throws IOException {
            Path pt = new Path(SIDE_DATA_PATH + "/part-r-00000");
            PairOfStrings key = new PairOfStrings();
            IntWritable val = new IntWritable();
            SequenceFile.Reader sequenceFileReader = new SequenceFile.Reader(context.getConfiguration(), SequenceFile.Reader.file(pt));

            while (sequenceFileReader.next(key, val)) {
                String leftWord = key.getLeftElement();
                float occurrenceCount = Float.parseFloat(val.toString());
                if (leftWord.equals("*")) {
                    WordOccurrences.put(TOTAL_NUMBER_OF_LINES, occurrenceCount);
                } else {
                    WordOccurrences.put(leftWord, occurrenceCount);
                }
            }
        }


        @Override
        public void reduce(Text key, Iterable<HMapStFW> values, Context context)
                throws IOException, InterruptedException {

            Iterator<HMapStFW> iter = values.iterator();
            HMapStFW map = new HMapStFW();
            HMapStFW filteredMap = new HMapStFW();


            while (iter.hasNext()) {
                map.plus(iter.next());
            }

            for (String term : map.keySet()) {
                float cooccurrenceNumber = map.get(term);
                if (cooccurrenceNumber >= 10) {
                    filteredMap.put(term, cooccurrenceNumber);
                }
            }

            double numberOfLines = WordOccurrences.get(TOTAL_NUMBER_OF_LINES);

//            for (String term : map.keySet()) {
//                float cooccurrenceNumber = map.get(term);
//                if (cooccurrenceNumber < 10) {
//                    map.remove(term);
//                }
//            }
//            int i = 0;
//            for (String key1 : WordOccurrences.keySet()) {
//                i++;
//                LOG.info("The key is -> " + key1 + " and the value is -> " + WordOccurrences.get(key1));
//                LOG.info("The reduce key is -> " + key + " and the occurrence count is -> " + WordOccurrences.get(key.toString()));
//
//                if (i == 10) break;
//            }

            // Number of times (a, *) occurs. But this is not necessary as it is done in word occurrences
//            for (MapKF.Entry<String> entry : map.entrySet()) {
//                sum += entry.getValue();
//            }

            for (String term : filteredMap.keySet()) {
                float cooccurrenceNumber = map.get(term);
                float XOccurrences = WordOccurrences.get(key.toString());
                float YOccurrences = WordOccurrences.get(term);
                float Pmi = (float) Math.log10((filteredMap.get(term) * numberOfLines) / (XOccurrences * YOccurrences));
                filteredMap.put(term, Pmi);

            }
            if (!filteredMap.isEmpty()) {
                context.write(key, filteredMap);
            }
        }
    }

    /**
     * Creates an instance of this tool.
     */
    public StripesPMI() {}

    public static class Args {
        @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
        public String input;

        @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
        public String output;

        @Option(name = "-reducers", metaVar = "[num]", required = false, usage = "number of reducers")
        public int numReducers;

    }

    /**
     * Runs this tool.
     */
    public int run(String[] argv) throws Exception {
        Args args = new Args();
        CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));
        final int FIRST_JOB_REDUCER_NUMBER = 1;

        try {
            parser.parseArgument(argv);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            return -1;
        }

        LOG.info("Tool: " + StripesPMI.class.getSimpleName());
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output path: " + args.output);
        LOG.info(" - number of reducers: " + args.numReducers);

        Configuration conf = getConf();

        // First job generates a word count
        Job firstJob = Job.getInstance(conf);
        firstJob.setJobName(StripesPMI.class.getSimpleName());
        firstJob.setJarByClass(StripesPMI.class);

        firstJob.setNumReduceTasks(FIRST_JOB_REDUCER_NUMBER);

        FileInputFormat.setInputPaths(firstJob, new Path(args.input));
        FileOutputFormat.setOutputPath(firstJob, new Path(SIDE_DATA_PATH));

        firstJob.setMapOutputKeyClass(PairOfStrings.class);
        firstJob.setMapOutputValueClass(IntWritable.class);
        firstJob.setOutputKeyClass(PairOfStrings.class);
        firstJob.setOutputValueClass(IntWritable.class);
        firstJob.setOutputFormatClass(SequenceFileOutputFormat.class);

        firstJob.setMapperClass(FirstJobMapper.class);
        firstJob.setCombinerClass(FirstJobCombiner.class);
        firstJob.setReducerClass(FirstJobReducer.class);

        // Delete the output directory if it exists already.
        Path tempOutputDir = new Path(SIDE_DATA_PATH);
        FileSystem.get(conf).delete(tempOutputDir, true);

        long startTimeForFirstJob = System.currentTimeMillis();
        firstJob.waitForCompletion(true);
        LOG.info("First Job Finished in " + (System.currentTimeMillis() - startTimeForFirstJob) / 1000.0 + " seconds");

        // Second job calculates the final PMI
        Job secondJob = Job.getInstance(conf);
        secondJob.setJobName(StripesPMI.class.getSimpleName());
        secondJob.setJarByClass(StripesPMI.class);

        // Increase memory because assignment told me to do
        secondJob.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 64);
        secondJob.getConfiguration().set("mapreduce.map.memory.mb", "3072");
        secondJob.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
        secondJob.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
        secondJob.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

        secondJob.setNumReduceTasks(args.numReducers);

        FileInputFormat.setInputPaths(secondJob, new Path(args.input));
        FileOutputFormat.setOutputPath(secondJob, new Path(args.output));

        secondJob.setMapOutputKeyClass(Text.class);
        secondJob.setMapOutputValueClass(HMapStFW.class);
        secondJob.setOutputKeyClass(Text.class);
        secondJob.setOutputValueClass(HMapStFW.class);

        secondJob.setMapperClass(SecondJobMapper.class);
        secondJob.setCombinerClass(SecondJobCombiner.class);
        secondJob.setReducerClass(SecondJobReducer.class);

        // Delete the output directory if it exists already.
        Path outputDir = new Path(args.output);
        FileSystem.get(conf).delete(outputDir, true);

        long startTimeForSecondJob = System.currentTimeMillis();
        secondJob.waitForCompletion(true);
        LOG.info("Second Job Finished in " + (System.currentTimeMillis() - startTimeForSecondJob) / 1000.0 + " seconds");

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new StripesPMI(), args);
    }
}
