package ca.uwaterloo.cs.bigdata2016w.esnrRahman.assignment3;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfWritables;

public class BooleanRetrievalCompressed extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(BuildInvertedIndexCompressed.class);

  private MapFile.Reader index;
  private FSDataInputStream collection;
  private Stack<Set<Integer>> stack;
  private String mainIndexPath;
  private ContentSummary contentSummary;
  private FileSystem mainFs;

  private BooleanRetrievalCompressed() {}

  private void initialize(String indexPath, String collectionPath, FileSystem fs) throws IOException {

    mainFs = fs;
    mainIndexPath = indexPath;
    Path pt = new Path(mainIndexPath);
    contentSummary = mainFs.getContentSummary(pt);
    collection =  mainFs.open(new Path(collectionPath));
    stack = new Stack<Set<Integer>>();
  }

  private void runQuery(String q) throws IOException {
    String[] terms = q.split("\\s+");

    for (String t : terms) {
      if (t.equals("AND")) {
        performAND();
      } else if (t.equals("OR")) {
        performOR();
      } else {
        pushTerm(t);
      }
    }

    Set<Integer> set = stack.pop();

    for (Integer i : set) {
      String line = fetchLine(i);
      System.out.println(i + "\t" + line);
    }
  }

  private void pushTerm(String term) throws IOException {
    stack.push(fetchDocumentSet(term));
  }

  private void performAND() {
    Set<Integer> s1 = stack.pop();
    Set<Integer> s2 = stack.pop();

    Set<Integer> sn = new TreeSet<Integer>();

    for (int n : s1) {
      if (s2.contains(n)) {
        sn.add(n);
      }
    }

    stack.push(sn);
  }

  private void performOR() {
    Set<Integer> s1 = stack.pop();
    Set<Integer> s2 = stack.pop();

    Set<Integer> sn = new TreeSet<Integer>();

    for (int n : s1) {
      sn.add(n);
    }

    for (int n : s2) {
      sn.add(n);
    }

    stack.push(sn);
  }

  private Set<Integer> fetchDocumentSet(String term) throws IOException {
    Set<Integer> set = new TreeSet<Integer>();

    for (PairOfInts pair : fetchPostings(term)) {
      set.add(pair.getLeftElement());
    }

    return set;
  }

  private ArrayListWritable<PairOfInts> fetchPostings(String term) throws IOException {

    long fileCount = contentSummary.getDirectoryCount();
    int numOfReducers = (int) fileCount - 1;

    int reducerNumber = (term.hashCode() & Integer.MAX_VALUE) % numOfReducers;

    index = new MapFile.Reader(new Path(mainIndexPath + "/part-r-0000" + reducerNumber), mainFs.getConf());

    Text key = new Text();
    BytesWritable value =
        new BytesWritable();
    //IntWritable df = new IntWritable();
    ArrayListWritable<PairOfInts> postings = new ArrayListWritable<PairOfInts>();
    int docId = 0;

    key.set(term);
    index.get(key, value);

    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(value.getBytes());
    DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
    int df = WritableUtils.readVInt(dataInputStream);
    //df.set(value.get());
    for (int i = 0; i < df; i++) {
      int gappedDocId = WritableUtils.readVInt(dataInputStream);
      int tf = WritableUtils.readVInt(dataInputStream);
      docId = gappedDocId + docId;
      postings.add(new PairOfInts(docId, tf));
    }
    return postings;
  }

  public String fetchLine(long offset) throws IOException {
    collection.seek(offset);
    BufferedReader reader = new BufferedReader(new InputStreamReader(collection));

    String d = reader.readLine();
    return d.length() > 80 ? d.substring(0, 80) + "..." : d;
  }

  public static class Args {
    @Option(name = "-index", metaVar = "[path]", required = true, usage = "index path")
    public String index;

    @Option(name = "-collection", metaVar = "[path]", required = true, usage = "collection path")
    public String collection;

    @Option(name = "-query", metaVar = "[term]", required = true, usage = "query")
    public String query;
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

    if (args.collection.endsWith(".gz")) {
      System.out.println("gzipped collection is not seekable: use compressed version!");
      return -1;
    }

    FileSystem fs = FileSystem.get(new Configuration());
    initialize(args.index, args.collection, fs);

    System.out.println("Query: " + args.query);
    long startTime = System.currentTimeMillis();
    runQuery(args.query);
    System.out.println("\nquery completed in " + (System.currentTimeMillis() - startTime) + "ms");

    return 1;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BooleanRetrievalCompressed(), args);
  }
}
