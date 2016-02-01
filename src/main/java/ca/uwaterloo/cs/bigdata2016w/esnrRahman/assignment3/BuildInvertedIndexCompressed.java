package ca.uwaterloo.cs.bigdata2016w.esnrRahman.assignment3;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfObjectInt;
import tl.lin.data.pair.PairOfStringInt;
import tl.lin.data.pair.PairOfWritables;

public class BuildInvertedIndexCompressed extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(BuildInvertedIndexCompressed.class);

    private static class MyMapper extends Mapper<LongWritable, Text, PairOfStringInt, IntWritable> {
        private static String WORD = "";
        private static final Object2IntFrequencyDistribution<String> COUNTS =
                new Object2IntFrequencyDistributionEntry<String>();
        private static final PairOfStringInt KEYPAIR = new PairOfStringInt();
        private static int DOCID = -1;
        private static final IntWritable TF = new IntWritable();

        @Override
        public void map(LongWritable docno, Text doc, Context context)
                throws IOException, InterruptedException {
            String text = doc.toString();

            // Tokenize line.
            List<String> tokens = new ArrayList<String>();
            StringTokenizer itr = new StringTokenizer(text);
            while (itr.hasMoreTokens()) {
                String w = itr.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
                if (w.length() == 0) continue;
                tokens.add(w);
            }

            // Build a histogram of the terms.
            COUNTS.clear();
            for (String token : tokens) {
                COUNTS.increment(token);
            }

            // Emit postings in the form of ((term, df), tf)
            for (PairOfObjectInt<String> e : COUNTS) {
                WORD = (e.getLeftElement()).toString();
                DOCID = (int) docno.get();
                TF.set(e.getRightElement());
                KEYPAIR.set(WORD, DOCID);
                context.write(KEYPAIR, TF);
            }
        }
    }

    protected static class MyPartitioner extends Partitioner<PairOfStringInt, IntWritable> {
        @Override
        public int getPartition(PairOfStringInt keyPair, IntWritable value, int numReduceTasks) {
            return (keyPair.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    private static class MyReducer extends
            Reducer<PairOfStringInt, IntWritable, Text, PairOfWritables<IntWritable, BytesWritable>> {
        private final static IntWritable DF = new IntWritable();
        private static final Text PREVTERM = new Text();
        private static final Text CURRTERM = new Text();
        private static int dGap = 0;
        private static int lastDocId = 0;
        private static boolean FLAG = true;
        private static final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        private static final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
        private static int documentFrequency = 0;

        @Override
        public void reduce(PairOfStringInt keyPair, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            if (FLAG) {
                PREVTERM.set(keyPair.getLeftElement());
                FLAG = false;
            }

            CURRTERM.set(keyPair.getLeftElement());

            if (!CURRTERM.equals(PREVTERM)) {
                DF.set(documentFrequency);
                BytesWritable bytesWritable = new BytesWritable(byteArrayOutputStream.toByteArray());
                context.write(CURRTERM, new PairOfWritables<IntWritable, BytesWritable>(DF, bytesWritable));

                dGap = 0;
                lastDocId = 0;
                documentFrequency = 0;
                byteArrayOutputStream.reset();
            }

            // Append in the BytesWritable. Equivalent to P.APPEND(<n ,f>)
            for (IntWritable tf : values) {
                documentFrequency += 1;
                dGap = keyPair.getRightElement() - lastDocId;
                lastDocId = keyPair.getRightElement();
                WritableUtils.writeVInt(outputStream, dGap);
                WritableUtils.writeVInt(outputStream, tf.get());
            }
            PREVTERM.set(CURRTERM.toString());

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(CURRTERM, new PairOfWritables<IntWritable, BytesWritable>(new IntWritable(documentFrequency),
                    new BytesWritable(byteArrayOutputStream.toByteArray())));
            byteArrayOutputStream.reset();
            super.cleanup(context);
        }
    }

    private BuildInvertedIndexCompressed() {
    }

    public static class Args {
        @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
        public String input;

        @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
        public String output;

        @Option(name = "-reducers", metaVar = "[num]", required = false, usage = "number of reducers")
        public int numReducers = 1;

    }

    /**
     * Runs this tool.
     */
    public int run(String[] argv) throws Exception {
        Args args = new Args();
        CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

        try {
            parser.parseArgument(argv);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            return -1;
        }

        LOG.info("Tool: " + BuildInvertedIndexCompressed.class.getSimpleName());
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output path: " + args.output);
        LOG.info(" - num reducers: " + args.numReducers);

        Job job = Job.getInstance(getConf());
        job.setJobName(BuildInvertedIndexCompressed.class.getSimpleName());
        job.setJarByClass(BuildInvertedIndexCompressed.class);

        job.setNumReduceTasks(args.numReducers);

        FileInputFormat.setInputPaths(job, new Path(args.input));
        FileOutputFormat.setOutputPath(job, new Path(args.output));

        job.setMapOutputKeyClass(PairOfStringInt.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PairOfWritables.class);
        job.setOutputFormatClass(MapFileOutputFormat.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setPartitionerClass(MyPartitioner.class);

        // Delete the output directory if it exists already.
        Path outputDir = new Path(args.output);
        FileSystem.get(getConf()).delete(outputDir, true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new BuildInvertedIndexCompressed(), args);
    }
}
