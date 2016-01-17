package ca.uwaterloo.cs.bigdata2016w.esnrRahman.assignment1;

import com.google.common.collect.Sets;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import scala.Int;
import tl.lin.data.pair.PairOfStrings;

public class PairsPMI extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(PairsPMI.class);
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
    private static class SecondJobMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
        // Reuse objects to save overhead of object creation.
        private static final PairOfStrings PAIR = new PairOfStrings();
        private static final IntWritable ONE = new IntWritable(1);
        private int window = 2;

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

            // Do co-occurrence pair count
//            for (int i = 0; i < tokens.size(); i++) {
//                for (int j = Math.max(i - window, 0); j < Math.min(i + window + 1, tokens.size()); j++) {
//                    if (i == j) continue;
//                    PAIR.set(tokens.get(i), tokens.get(j));
//                    context.write(PAIR, ONE);
//                }
//            }
            for(int i = 0; i < set.size(); i++) {
                for(int j = 0; j < set.size(); j++) {

                    String leftPair = set.get(i);
                    String rightPair = set.get(j);
                    if (leftPair.equals(rightPair)) continue;
                    PAIR.set(leftPair,rightPair);
                    context.write(PAIR, ONE);
                }
            }
        }
    }




    private static class SecondJobReducer extends
            Reducer<PairOfStrings, IntWritable, PairOfStrings, DoubleWritable> {
        private final static DoubleWritable PMI = new DoubleWritable();
        private static Map<String, Integer> WordOccurrences = new HashMap<String, Integer>();

        // NOTE: Took reference from the link below to read file from HDFS -
        // http://stackoverflow.com/questions/26209773/hadoop-map-reduce-read-a-text-file
        @Override
        public void setup(Context context) throws IOException {
            Path pt = new Path(SIDE_DATA_PATH + "/part-r-00000");
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            line = br.readLine();
            String w;
            String stringKey = "";
            String integerStrValue;
            int integerValue = -1;
            while (line != null) {
                StringTokenizer itr = new StringTokenizer(line);
                int i = 0;
                while (itr.hasMoreTokens()) {
                    w = itr.nextToken().toLowerCase();
                    switch(i) {
                        case 0:
                            w = w.replace("(", "");
                            stringKey = w.replace(",", "");
                            break;
                        case 1:
                            break;
                        case 2:
                            integerStrValue = w.replace(",", "");
                            integerValue = Integer.parseInt(integerStrValue);
                            break;
                        default:
                            break;
                    }
                    i++;
                }
                if (stringKey.equals("*")) {
                    WordOccurrences.put(TOTAL_NUMBER_OF_LINES, integerValue);
                } else {
                    WordOccurrences.put(stringKey, integerValue);
                }
                line = br.readLine();
            }
        }


        @Override
        public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            Iterator<IntWritable> iter = values.iterator();
            int sum = 0;
            while (iter.hasNext()) {
                sum += iter.next().get();
            }
            if (sum >= 10) {
                // Pmi calculation
                double numberOfLines = WordOccurrences.get(TOTAL_NUMBER_OF_LINES);
                double XOccurrences = WordOccurrences.get(key.getLeftElement());
                double YOccurrences = WordOccurrences.get(key.getRightElement());
                double Pmi = Math.log10((sum * numberOfLines) / (XOccurrences * YOccurrences));

                PMI.set(Pmi);
                context.write(key, PMI);
            }

        }
    }

    /**
     * Creates an instance of this tool.
     */
    public PairsPMI() {}

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

        LOG.info("Tool: " + PairsPMI.class.getSimpleName());
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output path: " + args.output);
        LOG.info(" - number of reducers: " + args.numReducers);

        Configuration conf = getConf();

        // First job generates a word count
        Job firstJob = Job.getInstance(conf);
        firstJob.setJobName(PairsPMI.class.getSimpleName());
        firstJob.setJarByClass(PairsPMI.class);

        firstJob.setNumReduceTasks(FIRST_JOB_REDUCER_NUMBER);

        FileInputFormat.setInputPaths(firstJob, new Path(args.input));
        FileOutputFormat.setOutputPath(firstJob, new Path(SIDE_DATA_PATH));

        firstJob.setMapOutputKeyClass(PairOfStrings.class);
        firstJob.setMapOutputValueClass(IntWritable.class);
        firstJob.setOutputKeyClass(PairOfStrings.class);
        firstJob.setOutputValueClass(IntWritable.class);

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
        secondJob.setJobName(PairsPMI.class.getSimpleName());
        secondJob.setJarByClass(PairsPMI.class);

        secondJob.setNumReduceTasks(args.numReducers);

        FileInputFormat.setInputPaths(secondJob, new Path(args.input));
        FileOutputFormat.setOutputPath(secondJob, new Path(args.output));

        secondJob.setMapOutputKeyClass(PairOfStrings.class);
        secondJob.setMapOutputValueClass(IntWritable.class);
        secondJob.setOutputKeyClass(PairOfStrings.class);
        secondJob.setOutputValueClass(DoubleWritable.class);

        secondJob.setMapperClass(SecondJobMapper.class);
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
        ToolRunner.run(new PairsPMI(), args);
    }
}
