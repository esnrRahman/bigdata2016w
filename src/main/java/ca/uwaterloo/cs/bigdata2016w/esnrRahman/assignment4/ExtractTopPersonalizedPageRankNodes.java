package ca.uwaterloo.cs.bigdata2016w.esnrRahman.assignment4;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import scala.Int;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfObjectFloat;
import tl.lin.data.queue.TopScoredObjects;

public class ExtractTopPersonalizedPageRankNodes extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ExtractTopPersonalizedPageRankNodes.class);

  private static final ArrayList<Integer> sourceNodes = new ArrayList<>();

  private static boolean isSourceNode(int n) {
    for (int i = 0; i < sourceNodes.size(); i++) {
      if (n == sourceNodes.get(0)) {
        return true;
      }
    }
    return false;
  }

  private static class MyMapper extends
      Mapper<IntWritable, PageRankNode, PairOfInts, FloatWritable> {
    private ArrayList<TopScoredObjects<Integer>> queueList;
    // Creating a pair of source id and node id
    private PairOfInts sourceNodePair = new PairOfInts();

    @Override
    public void setup(Context context) throws IOException {
      int k = context.getConfiguration().getInt("n", 100);
      queueList = new ArrayList<>();

      for (int i = 0; i < sourceNodes.size(); i++) {
        queueList.add(new TopScoredObjects<Integer>(k));
      }

    }

    @Override
    public void map(IntWritable nid, PageRankNode node, Context context) throws IOException,
        InterruptedException {

      for (int i = 0; i < sourceNodes.size(); i++) {
        queueList.get(i).add(node.getNodeId(), node.getPageRank().get(i));
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      IntWritable key = new IntWritable();
      FloatWritable value = new FloatWritable();

      for (int i = 0; i < sourceNodes.size(); i++) {
        for (PairOfObjectFloat<Integer> pair : queueList.get(i).extractAll()) {
          sourceNodePair.set(pair.getLeftElement(), i);
          value.set(pair.getRightElement());
          context.write(sourceNodePair, value);
        }
      }
    }
  }

  private static class MyReducer extends
      Reducer<PairOfInts, FloatWritable, IntWritable, FloatWritable> {
    private static ArrayList<TopScoredObjects<Integer>> queueList;

    @Override
    public void setup(Context context) throws IOException {
      int k = context.getConfiguration().getInt("n", 100);
      queueList = new ArrayList<>();

      for (int i = 0; i < sourceNodes.size(); i++) {
        queueList.add(new TopScoredObjects<Integer>(k));
      }
    }

    @Override
    public void reduce(PairOfInts sourceNodePair, Iterable<FloatWritable> iterable, Context context)
        throws IOException {
      Iterator<FloatWritable> iter = iterable.iterator();
      queueList.get(sourceNodePair.getRightElement()).add
              (sourceNodePair.getLeftElement(), iter.next().get());

      // Shouldn't happen. Throw an exception.
      if (iter.hasNext()) {
        throw new RuntimeException();
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      IntWritable key = new IntWritable();
      FloatWritable value = new FloatWritable();

      for (int i = 0; i < sourceNodes.size(); i++) {
        for (PairOfObjectFloat<Integer> pair : queueList.get(i).extractAll()) {
          key.set(pair.getLeftElement());
          // Need to take e^val because its in log
          value.set((float) StrictMath.exp(pair.getRightElement()));
          context.write(key, value);
        }
      }

    }
  }

  public ExtractTopPersonalizedPageRankNodes() {
  }

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String TOP = "top";
  private static final String SOURCES = "sources";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("top n").create(TOP));
    options.addOption(OptionBuilder.withArgName("source nodes").hasArg()
            .withDescription("source nodes").create(SOURCES));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) ||
        !cmdline.hasOption(TOP) || !cmdline.hasOption(SOURCES)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String actualOutputPath = cmdline.getOptionValue(OUTPUT);
    int n = Integer.parseInt(cmdline.getOptionValue(TOP));
    String sourceNodesStr = cmdline.getOptionValue(SOURCES);
    String tempOutputPath = "unformattedDir";

    // Get the source nodes
    ArrayList<String> sourceStrings = new ArrayList<>();
    sourceStrings.addAll(Arrays.asList(sourceNodesStr.split(",")));
    for (int i = 0; i < sourceStrings.size(); i++) {
      sourceNodes.add(Integer.parseInt(sourceStrings.get(i)));
    }

    LOG.info("Tool name: " + ExtractTopPersonalizedPageRankNodes.class.getSimpleName());
    LOG.info(" - input: " + inputPath);
    LOG.info(" - actual output: " + actualOutputPath);
    LOG.info(" - temp output: " + tempOutputPath);
    LOG.info(" - top: " + n);

    Configuration conf = getConf();
    conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);
    conf.setInt("n", n);

    Job job = Job.getInstance(conf);
    job.setJobName(ExtractTopPersonalizedPageRankNodes.class.getName() + ":" + inputPath);
    job.setJarByClass(ExtractTopPersonalizedPageRankNodes.class);

    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(tempOutputPath));

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapOutputKeyClass(PairOfInts.class);
    job.setMapOutputValueClass(FloatWritable.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(FloatWritable.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    FileSystem fs = FileSystem.get(conf);

    // Delete the output directory if it exists already.
    fs.delete(new Path(tempOutputPath), true);
    fs.delete(new Path(actualOutputPath), true);

    job.waitForCompletion(true);

    Path writePath = new Path(actualOutputPath);
    fs.mkdirs(writePath);
    Path readPath = new Path(tempOutputPath + "/part-r-00000");
    writePath = new Path(actualOutputPath + "/part-r-00000");
    BufferedReader readBr = new BufferedReader(new InputStreamReader(fs.open(readPath)));
    BufferedWriter writeBr = new BufferedWriter(new OutputStreamWriter(fs.create(writePath, true)));
    String readLine;
    String writeLine = "";
    readLine = readBr.readLine();
    int count = 0;
    boolean firstTime = true;
    DecimalFormat fiveDForm = new DecimalFormat("#.##");
    while(readLine != null) {
      String[] setOfWords = readLine.split("\\t");
      String firstWord = setOfWords[0];
      String secondWord = setOfWords[1];
      if (count == n || firstTime) {
        writeLine = "Source: " + firstWord + "\n";
        count = 1;
        firstTime = false;
      } else {
        float f = Float.parseFloat(secondWord);
        writeLine = Float.valueOf(fiveDForm.format(f)) + " " + firstWord + "\n";
        count++;
        if (count == n) {
          writeLine += "\n";
        }
      }
      writeBr.write(writeLine);
      readLine = readBr.readLine();
    }
    readBr.close();
    writeBr.close();
    fs.delete(new Path(tempOutputPath), true);
    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new ExtractTopPersonalizedPageRankNodes(), args);
    System.exit(res);
  }
}
