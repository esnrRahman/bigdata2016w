package ca.uwaterloo.cs.bigdata2016w.esnrRahman.assignment4;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tl.lin.data.array.ArrayListOfFloatsWritable;
import tl.lin.data.array.ArrayListOfIntsWritable;
import tl.lin.data.map.HMapIF;
import tl.lin.data.map.MapIF;

import com.google.common.base.Preconditions;

/**
 * <p>
 * Main driver program for running the basic (non-Schimmy) implementation of
 * PageRank.
 * </p>
 *
 * <p>
 * The starting and ending iterations will correspond to paths
 * <code>/base/path/iterXXXX</code> and <code>/base/path/iterYYYY</code>. As a
 * example, if you specify 0 and 10 as the starting and ending iterations, the
 * driver program will start with the graph structure stored at
 * <code>/base/path/iter0000</code>; final results will be stored at
 * <code>/base/path/iter0010</code>.
 * </p>
 *
 * @author Jimmy Lin
 * @author Michael Schatz
 */
public class RunPersonalizedPageRankBasic extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(RunPersonalizedPageRankBasic.class);

//  private static final ArrayList<Integer> sourceNodes = new ArrayList<>();

//  private static boolean isSourceNode(int n) {
//    for (int i = 0; i < sourceNodes.size(); i++) {
//      if (n == sourceNodes.get(i)) {
//        return true;
//      }
//    }
//    return false;
//  }

  private static enum PageRank {
    nodes, edges, massMessages, massMessagesSaved, massMessagesReceived, missingStructure
  };

  // Mapper, no in-mapper combining.
  private static class MapClass extends
      Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {

    // The neighbor to which we're sending messages.
    private static final IntWritable neighbor = new IntWritable();

    // Contents of the messages: partial PageRank mass.
    private static final PageRankNode intermediateMass = new PageRankNode();

    // For passing along node structure.
    private static final PageRankNode intermediateStructure = new PageRankNode();

    private static int sourceNodesSize = -1;

    @Override
    public void setup(Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode>.Context context) {
      // Get source node information from context
      String sourceNodesArray[] = context.getConfiguration().getStrings("sourceNodes");
      sourceNodesSize = sourceNodesArray.length;

//      LOG.info("EHSAN POINT 11: Size is -> " + sourceNodesSize);

    }


    @Override
    public void map(IntWritable nid, PageRankNode node, Context context)
        throws IOException, InterruptedException {

      ArrayListOfFloatsWritable massList = new ArrayListOfFloatsWritable();

      // Pass along node structure.
      intermediateStructure.setNodeId(node.getNodeId());
      intermediateStructure.setType(PageRankNode.Type.Structure);
      intermediateStructure.setAdjacencyList(node.getAdjacenyList());

      context.write(nid, intermediateStructure);

      int massMessages = 0;

      // Distribute PageRank mass to neighbors (along outgoing edges).
      if (node.getAdjacenyList().size() > 0) {

        // Each neighbor gets an equal share of PageRank mass.
        ArrayListOfIntsWritable list = node.getAdjacenyList();

        context.getCounter(PageRank.edges).increment(list.size());

        for (int i = 0; i < sourceNodesSize; i++) {
          massList.add(node.getPageRank().get(i) - (float) StrictMath.log(list.size()));
        }

        // Iterate over neighbors.
        for (int i = 0; i < list.size(); i++) {
          neighbor.set(list.get(i));
          intermediateMass.setNodeId(list.get(i));
          intermediateMass.setType(PageRankNode.Type.Mass);
          intermediateMass.setPageRank(massList);

          // Emit messages with PageRank mass to neighbors.
          context.write(neighbor, intermediateMass);
          massMessages++;
        }
      }

      // Bookkeeping.
      context.getCounter(PageRank.nodes).increment(1);
      context.getCounter(PageRank.massMessages).increment(massMessages);
    }
  }

  // ==================== XXX: DONT GIVE A SHIT TERRITORY ===========================

  // Mapper with in-mapper combiner optimization.
  private static class MapWithInMapperCombiningClass extends
      Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {
    // For buffering PageRank mass contributes keyed by destination node.
    private static final HMapIF map = new HMapIF();

    // For passing along node structure.
    private static final PageRankNode intermediateStructure = new PageRankNode();

    private static int sourceNodesSize = -1;


    @Override
    public void setup(Context context) throws IOException {
      // Note that this is needed for running in local mode due to mapper reuse.
      // We wouldn't need this is distributed mode.
      String sourceNodesArray[] = context.getConfiguration().getStrings("sourceNodes");
      sourceNodesSize = sourceNodesArray.length;

      map.clear();
    }

    @Override
    public void map(IntWritable nid, PageRankNode node, Context context)
        throws IOException, InterruptedException {
      // NOTE: All changes related to massList does not make sense to me.
      // The only reason I did it is just to remove compilationErrors. I do not
      // know what is happening in this side of the code. Seriously need to get rid of this
      ArrayListOfFloatsWritable massList = new ArrayListOfFloatsWritable();

      // Pass along node structure.
      intermediateStructure.setNodeId(node.getNodeId());
      intermediateStructure.setType(PageRankNode.Type.Structure);
      intermediateStructure.setAdjacencyList(node.getAdjacenyList());

      context.write(nid, intermediateStructure);

      int massMessages = 0;
      int massMessagesSaved = 0;

      // Distribute PageRank mass to neighbors (along outgoing edges).
      if (node.getAdjacenyList().size() > 0) {
        // Each neighbor gets an equal share of PageRank mass.
        ArrayListOfIntsWritable list = node.getAdjacenyList();

        for (int i = 0; i < sourceNodesSize; i++) {
          massList.add(node.getPageRank().get(i) - (float) StrictMath.log(list.size()));
//          LOG.info("EHSAN POINT 1 -> " + massList.get(i));
        }

        context.getCounter(PageRank.edges).increment(list.size());

        // Iterate over neighbors.
        for (int i = 0; i < list.size(); i++) {
          int neighbor = list.get(i);

          if (map.containsKey(neighbor)) {
            // Already message destined for that node; add PageRank mass contribution.
            massMessagesSaved++;
            map.put(neighbor, sumLogProbs(map.get(neighbor), massList.get(i)));
          } else {
            // New destination node; add new entry in map.
            massMessages++;
            map.put(neighbor, massList.get(i));
          }
        }
      }

      // Bookkeeping.
      context.getCounter(PageRank.nodes).increment(1);
      context.getCounter(PageRank.massMessages).increment(massMessages);
      context.getCounter(PageRank.massMessagesSaved).increment(massMessagesSaved);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      // Now emit the messages all at once.
      IntWritable k = new IntWritable();
      PageRankNode mass = new PageRankNode();

      for (MapIF.Entry e : map.entrySet()) {
        k.set(e.getKey());

        mass.setNodeId(e.getKey());
        mass.setType(PageRankNode.Type.Mass);
//        mass.setPageRank(e.getValue());

        context.write(k, mass);
      }
    }
  }

  // ===================================================================================

  // Combiner: sums partial PageRank contributions and passes node structure along.
  private static class CombineClass extends
      Reducer<IntWritable, PageRankNode, IntWritable, PageRankNode> {
    private static final PageRankNode intermediateMass = new PageRankNode();

    private static int sourceNodesSize = -1;

    @Override
    public void setup(Reducer<IntWritable, PageRankNode, IntWritable, PageRankNode>.Context context) {
      // Get source node information from context
      String sourceNodesArray[] = context.getConfiguration().getStrings("sourceNodes");
      sourceNodesSize = sourceNodesArray.length;
    }

    @Override
    public void reduce(IntWritable nid, Iterable<PageRankNode> values, Context context)
        throws IOException, InterruptedException {
      int massMessages = 0;

      // Remember, PageRank mass is stored as a log prob.
      ArrayListOfFloatsWritable massList = new ArrayListOfFloatsWritable();

      for (int i = 0; i < sourceNodesSize; i++) {
        massList.add(Float.NEGATIVE_INFINITY);
      }

      for (PageRankNode n : values) {
        if (n.getType() == PageRankNode.Type.Structure) {
          // Simply pass along node structure.
          context.write(nid, n);
        } else {
          // Accumulate PageRank mass contributions.

          for (int i = 0; i < sourceNodesSize; i++) {
            massList.set(i, sumLogProbs(massList.get(i), n.getPageRank().get(i))) ;
//            LOG.info("EHSAN POINT 2 -> " + massList.get(i));
          }

          massMessages++;
        }
      }

      // Emit aggregated results.
      if (massMessages > 0) {
        intermediateMass.setNodeId(nid.get());
        intermediateMass.setType(PageRankNode.Type.Mass);
        intermediateMass.setPageRank(massList);

        context.write(nid, intermediateMass);
      }
    }
  }

  // ================== FURTHER EXTENDED THE DONT GIVE A SHIT TERRITORY ======================

  // Reduce: sums incoming PageRank contributions, rewrite graph structure.
  private static class ReduceClass extends
      Reducer<IntWritable, PageRankNode, IntWritable, PageRankNode> {
    // For keeping track of PageRank mass encountered, so we can compute missing PageRank mass lost
    // through dangling nodes.
    private ArrayListOfFloatsWritable totalMassList = new ArrayListOfFloatsWritable();

    private static int sourceNodesSize = -1;

    @Override
    public void setup(Reducer<IntWritable, PageRankNode, IntWritable, PageRankNode>.Context context) {
      // Get source node information from context
      String sourceNodesArray[] = context.getConfiguration().getStrings("sourceNodes");
      sourceNodesSize = sourceNodesArray.length;

      for (int i = 0; i < sourceNodesSize; i++) {
        totalMassList.add(Float.NEGATIVE_INFINITY);
      }
    }

    @Override
    public void reduce(IntWritable nid, Iterable<PageRankNode> iterable, Context context)
        throws IOException, InterruptedException {
      Iterator<PageRankNode> values = iterable.iterator();

      // Create the node structure that we're going to assemble back together from shuffled pieces.
      PageRankNode node = new PageRankNode();

      node.setType(PageRankNode.Type.Complete);
      node.setNodeId(nid.get());

      int massMessagesReceived = 0;
      int structureReceived = 0;

      ArrayListOfFloatsWritable massList = new ArrayListOfFloatsWritable();

      for (int i = 0; i < sourceNodesSize; i++) {
        massList.add(Float.NEGATIVE_INFINITY);
      }

      while (values.hasNext()) {
        PageRankNode n = values.next();

        if (n.getType().equals(PageRankNode.Type.Structure)) {
          // This is the structure; update accordingly.
          ArrayListOfIntsWritable list = n.getAdjacenyList();
          structureReceived++;

          node.setAdjacencyList(list);
        } else {
          // This is a message that contains PageRank mass; accumulate.

          for (int i = 0; i < sourceNodesSize; i++) {
            massList.set(i, sumLogProbs(massList.get(i), n.getPageRank().get(i)));
//            LOG.info("EHSAN POINT 3 -> " + massList.get(i));
          }

          massMessagesReceived++;
        }
      }

      // Update the final accumulated PageRank mass.
      node.setPageRank(massList);
      context.getCounter(PageRank.massMessagesReceived).increment(massMessagesReceived);

      // Error checking.
      if (structureReceived == 1) {
        // Everything checks out, emit final node structure with updated PageRank value.
        context.write(nid, node);

        // Keep track of total PageRank mass.
        for (int i = 0; i < sourceNodesSize; i++) {
          totalMassList.set(i, sumLogProbs(totalMassList.get(i), massList.get(i)));
//          LOG.info("EHSAN POINT 4 -> " + totalMassList.get(i));
        }

      } else if (structureReceived == 0) {
        // We get into this situation if there exists an edge pointing to a node which has no
        // corresponding node structure (i.e., PageRank mass was passed to a non-existent node)...
        // log and count but move on.
        context.getCounter(PageRank.missingStructure).increment(1);
        LOG.warn("No structure received for nodeid: " + nid.get() + " mass: "
            + massMessagesReceived);
        // It's important to note that we don't add the PageRank mass to total... if PageRank mass
        // was sent to a non-existent node, it should simply vanish.
      } else {
        // This shouldn't happen!
        throw new RuntimeException("Multiple structure received for nodeid: " + nid.get()
            + " mass: " + massMessagesReceived + " struct: " + structureReceived);
      }
    }

    @Override
    public void cleanup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      String taskId = conf.get("mapred.task.id");
      String path = conf.get("PageRankMassPath");

      Preconditions.checkNotNull(taskId);
      Preconditions.checkNotNull(path);

      // Write to a file the amount of PageRank mass we've seen in this reducer.
      FileSystem fs = FileSystem.get(context.getConfiguration());
      FSDataOutputStream out = fs.create(new Path(path + "/" + taskId), false);

      for (int i = 0; i < sourceNodesSize; i++) {
        out.writeFloat(totalMassList.get(i));
      }

      out.close();
    }
  }

  // Mapper that distributes the missing PageRank mass (lost at the dangling nodes) and takes care
  // of the random jump factor.
  private static class MapPageRankMassDistributionClass extends
      Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {
    private ArrayListOfFloatsWritable missingMassList = new ArrayListOfFloatsWritable();
    private int nodeCnt = 0;

    private static int sourceNodesSize = -1;

    private ArrayList<Integer> sourceNodes = new ArrayList<>();

    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();

      String[] missingMassListStr = context.getConfiguration().getStrings("MissingMass");
      nodeCnt = conf.getInt("NodeCount", 0);

      // Get source node information from context
      String sourceNodesArray[] = context.getConfiguration().getStrings("sourceNodes");
      sourceNodesSize = sourceNodesArray.length;

      //  Get the source nodes
      for (int i = 0; i < sourceNodesSize; i++) {
        sourceNodes.add(Integer.parseInt(sourceNodesArray[i]));
        missingMassList.add(Float.parseFloat(missingMassListStr[i]));
      }
    }

    @Override
    public void map(IntWritable nid, PageRankNode node, Context context)
        throws IOException, InterruptedException {
      ArrayListOfFloatsWritable p = node.getPageRank();

      // This is the formula in action
      for (int i = 0; i < sourceNodes.size(); i++) {
        if (sourceNodes.get(i) == nid.get()) {

          float jump = (float) (Math.log(ALPHA));
          float link = Float.NEGATIVE_INFINITY;
          if (missingMassList.get(i) < 0) {
            link = (float) Math.log(1.0f - ALPHA)
                    + sumLogProbs(p.get(i), (float) (Math.log(0)));
          } else {
            link = (float) Math.log(1.0f - ALPHA)
                    + sumLogProbs(p.get(i), (float) (missingMassList.get(i)));
          }
          p.set(i, sumLogProbs(jump, link));
        } else {
          p.set(i, p.get(i) + (float) Math.log(1.0f - ALPHA));
        }
      }

      node.setPageRank(p);

      context.write(nid, node);
    }
  }

  // Random jump factor.
  private static float ALPHA = 0.15f;
  private static NumberFormat formatter = new DecimalFormat("0000");

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new RunPersonalizedPageRankBasic(), args);
  }

  public RunPersonalizedPageRankBasic() {}

  private static final String BASE = "base";
  private static final String NUM_NODES = "numNodes";
  private static final String START = "start";
  private static final String END = "end";
  private static final String COMBINER = "useCombiner";
//  private static final String INMAPPER_COMBINER = "useInMapperCombiner";
  private static final String SOURCES = "sources";
  private static final String RANGE = "range";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(new Option(COMBINER, "use combiner"));
//    options.addOption(new Option(INMAPPER_COMBINER, "user in-mapper combiner"));
    options.addOption(new Option(RANGE, "use range partitioner"));

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("base path").create(BASE));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("start iteration").create(START));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("end iteration").create(END));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of nodes").create(NUM_NODES));
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

    if (!cmdline.hasOption(BASE) || !cmdline.hasOption(START) ||
        !cmdline.hasOption(END) || !cmdline.hasOption(NUM_NODES) ||
        !cmdline.hasOption(SOURCES)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String basePath = cmdline.getOptionValue(BASE);
    int n = Integer.parseInt(cmdline.getOptionValue(NUM_NODES));
    int s = Integer.parseInt(cmdline.getOptionValue(START));
    int e = Integer.parseInt(cmdline.getOptionValue(END));
    boolean useCombiner = false;
    // TODO: Remove inmapCombiner completely
    boolean useInmapCombiner = false;
    boolean useRange = cmdline.hasOption(RANGE);
    String sourceNodesStr = cmdline.getOptionValue(SOURCES);

    // Get the source nodes
//    ArrayList<String> sourceStrings = new ArrayList<>();
//    sourceStrings.addAll(Arrays.asList(sourceNodesStr.split(",")));
//    for (int i = 0; i < sourceStrings.size(); i++) {
//      sourceNodes.add(Integer.parseInt(sourceStrings.get(i)));
//    }

    LOG.info("Tool name: RunPageRank");
    LOG.info(" - base path: " + basePath);
    LOG.info(" - num nodes: " + n);
    LOG.info(" - start iteration: " + s);
    LOG.info(" - end iteration: " + e);
    LOG.info(" - use combiner: " + useCombiner);
    LOG.info(" - use in-mapper combiner: " + useInmapCombiner);
    LOG.info(" - user range partitioner: " + useRange);
    LOG.info(" - source nodes: " + sourceNodesStr);

    // Iterate PageRank.
    for (int i = s; i < e; i++) {
      iteratePageRank(i, i + 1, basePath, n, useCombiner, useInmapCombiner, sourceNodesStr);
    }

    return 0;
  }

  // Run each iteration.
  private void iteratePageRank(int i, int j, String basePath, int numNodes,
      boolean useCombiner, boolean useInMapperCombiner, String sourceNodesStr) throws Exception {

    int sourceNodesListSize = Arrays.asList(sourceNodesStr.split(",")).size();

//    LOG.info("EHSAN POINT 10: Size is -> " + sourceNodesListSize);

    ArrayListOfFloatsWritable massList;
    ArrayListOfFloatsWritable missingList = new ArrayListOfFloatsWritable();

    // Each iteration consists of two phases (two MapReduce jobs).

    // Job 1: distribute PageRank mass along outgoing edges.
    massList = phase1(i, j, basePath, numNodes, useCombiner, useInMapperCombiner, sourceNodesStr);

//    for (int k = 0; k < massList.size(); k++) {
//      LOG.info("EHSAN POINT 6 -> " + massList.get(k));
//    }

    // Find out how much PageRank mass got lost at the dangling nodes.
    for (int k = 0; k < sourceNodesListSize; k++) {
      missingList.add(1.0f - (float) StrictMath.exp(massList.get(k)));
    }

    String missingMassesStr = "";
    missingMassesStr += missingList.get(0);
    for (int k = 1; k < sourceNodesListSize; k++) {
      missingMassesStr += "," + missingList.get(k);
    }

//    LOG.info("EHSAN POINT 7 -> " + massList.toString());

//    LOG.info("EHSAN POINT 8 -> " + missingMassesStr);

    // Job 2: distribute missing mass, take care of random jump factor.
    phase2(i, j, missingMassesStr, basePath, numNodes, sourceNodesStr);
  }

  private ArrayListOfFloatsWritable phase1(int i, int j, String basePath, int numNodes,
      boolean useCombiner, boolean useInMapperCombiner, String sourceNodesStr) throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName("PageRank:Basic:iteration" + j + ":Phase1");
    job.setJarByClass(RunPersonalizedPageRankBasic.class);

    String in = basePath + "/iter" + formatter.format(i);
    String out = basePath + "/iter" + formatter.format(j) + "t";
    String outm = out + "-mass";

    int sourceNodesListSize = Arrays.asList(sourceNodesStr.split(",")).size();

    // We need to actually count the number of part files to get the number of partitions (because
    // the directory might contain _log).
    int numPartitions = 0;
    for (FileStatus s : FileSystem.get(getConf()).listStatus(new Path(in))) {
      if (s.getPath().getName().contains("part-"))
        numPartitions++;
    }

    LOG.info("PageRank: iteration " + j + ": Phase1");
    LOG.info(" - input: " + in);
    LOG.info(" - output: " + out);
    LOG.info(" - nodeCnt: " + numNodes);
    LOG.info(" - useCombiner: " + useCombiner);
    LOG.info(" - useInmapCombiner: " + useInMapperCombiner);
    LOG.info("computed number of partitions: " + numPartitions);

    int numReduceTasks = numPartitions;

    job.getConfiguration().setInt("NodeCount", numNodes);
    job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
    job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
    //job.getConfiguration().set("mapred.child.java.opts", "-Xmx2048m");
    job.getConfiguration().set("PageRankMassPath", outm);
    job.getConfiguration().setStrings("sourceNodes", sourceNodesStr);

    job.setNumReduceTasks(numReduceTasks);

    FileInputFormat.setInputPaths(job, new Path(in));
    FileOutputFormat.setOutputPath(job, new Path(out));

    job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(PageRankNode.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(PageRankNode.class);

    job.setMapperClass(useInMapperCombiner ? MapWithInMapperCombiningClass.class : MapClass.class);

    if (useCombiner) {
      job.setCombinerClass(CombineClass.class);
    }

    job.setReducerClass(ReduceClass.class);

    FileSystem.get(getConf()).delete(new Path(out), true);
    FileSystem.get(getConf()).delete(new Path(outm), true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    ArrayListOfFloatsWritable massList = new ArrayListOfFloatsWritable();

    for (int k = 0; k < sourceNodesListSize; k++) {
      massList.add(Float.NEGATIVE_INFINITY);
    }

    FileSystem fs = FileSystem.get(getConf());
    for (FileStatus f : fs.listStatus(new Path(outm))) {
      FSDataInputStream fin = fs.open(f.getPath());

      for (int k = 0; k < sourceNodesListSize; k++) {
        massList.set(k, sumLogProbs(massList.get(k), fin.readFloat()));
//        LOG.info("EHSAN POINT 9 -> " + massList.get(k));
      }

      fin.close();
    }

    return massList;
  }

  private void phase2(int i, int j, String missingMassesStr,
                      String basePath, int numNodes, String sourceNodesStr) throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName("PageRank:Basic:iteration" + j + ":Phase2");
    job.setJarByClass(RunPersonalizedPageRankBasic.class);

    LOG.info("missing PageRank mass: " + missingMassesStr);
    LOG.info("number of nodes: " + numNodes);

    String in = basePath + "/iter" + formatter.format(j) + "t";
    String out = basePath + "/iter" + formatter.format(j);

    LOG.info("PageRank: iteration " + j + ": Phase2");
    LOG.info(" - input: " + in);
    LOG.info(" - output: " + out);

    job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
    job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
    job.getConfiguration().setStrings("MissingMass", missingMassesStr);
    job.getConfiguration().setInt("NodeCount", numNodes);
    job.getConfiguration().setStrings("sourceNodes", sourceNodesStr);

    job.setNumReduceTasks(0);

    FileInputFormat.setInputPaths(job, new Path(in));
    FileOutputFormat.setOutputPath(job, new Path(out));

    job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(PageRankNode.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(PageRankNode.class);

    job.setMapperClass(MapPageRankMassDistributionClass.class);

    FileSystem.get(getConf()).delete(new Path(out), true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
  }

  // Adds two log probs.
  private static float sumLogProbs(float a, float b) {
    if (a == Float.NEGATIVE_INFINITY)
      return b;

    if (b == Float.NEGATIVE_INFINITY)
      return a;

    if (a < b) {
      return (float) (b + StrictMath.log1p(StrictMath.exp(a - b)));
    }

    return (float) (a + StrictMath.log1p(StrictMath.exp(b - a)));
  }
}
