package ca.uwaterloo.cs.bigdata2016w.esnrRahman.assignment7;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
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

public class BooleanRetrievalHBase extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(BooleanRetrievalHBase.class);

	private HTableInterface table;
	//	private MapFile.Reader index;
	private FSDataInputStream collection;
	private Stack<Set<Integer>> stack;


	private BooleanRetrievalHBase() {
	}

	private void initialize(String collectionPath, FileSystem fs) throws IOException {
//		index = new MapFile.Reader(new Path(indexPath + "/part-r-00000"), fs.getConf());
		collection = fs.open(new Path(collectionPath));
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

//		// Debugging
//		LOG.info("EHSAN 1 -> " + stack.size());
//		LOG.info("EHSAN 2 -> " + set.size());
//
//		for (Integer i : set) {
//			LOG.info("EHSAN 3 -> " + i);
//		}
//
//		for (Set<Integer> i : stack) {
//			LOG.info("EHSAN 4 -> " + i);
//		}

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

//		LOG.info("EHSAN 1");

		for (PairOfInts pair : fetchPostings(term)) {
//			LOG.info("EHSAN 2");
			set.add(pair.getLeftElement());
		}

		return set;
	}

	// Return Array (docId, tf)
	private ArrayListWritable<PairOfInts> fetchPostings(String term) throws IOException {

		NavigableMap<byte[], byte[]> qualifierValueMap;
		Get get = new Get(Bytes.toBytes(term));
		Result result = table.get(get);

		// byte[] getValue(byte[] family, byte[] qualifier)
//		int count = Bytes.toInt(result.getValue(HBaseWordCount.CF, HBaseWordCount.COUNT));
		qualifierValueMap = result.getFamilyMap("p".getBytes());

//		LOG.info("EHSAN 4 -> " + qualifierValueMap.size());

//		Text key = new Text();
//		BytesWritable value =
//						new BytesWritable();
		//IntWritable df = new IntWritable();
		ArrayListWritable<PairOfInts> postings = new ArrayListWritable<PairOfInts>();
//		int docId = 0;

//		key.set(term);
//		index.get(key, value);

//		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(value.getBytes());
//		DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
//		int df = WritableUtils.readVInt(dataInputStream);
//		//df.set(value.get());
		for(Map.Entry<byte[], byte[]> entry: qualifierValueMap.entrySet()) {
			postings.add(new PairOfInts(Bytes.toInt(entry.getKey()), Bytes.toInt(entry.getValue())));
//			LOG.info("EHSAN 5: docid -> " + Bytes.toInt(entry.getKey()) + " tf ->" + Bytes.toInt(entry.getValue()));
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
		@Option(name = "-table", metaVar = "[name]", required = true, usage = "HBase table to read")
		public String table;

		@Option(name = "-config", metaVar = "[path]", required = true, usage = "HBase config")
		public String config;

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

		LOG.info("Tool: " + BooleanRetrievalHBase.class.getSimpleName());
		LOG.info(" - collection path: " + args.collection);
		LOG.info(" - table that is being read: " + args.table);
		LOG.info(" - config: " + args.config);
		LOG.info(" - query: " + args.query);

		if (args.collection.endsWith(".gz")) {
			System.out.println("gzipped collection is not seekable: use compressed version!");
			return -1;
		}

		FileSystem fs = FileSystem.get(new Configuration());

		Configuration conf = getConf();
		conf.addResource(new Path(args.config));

		Configuration hbaseConfig = HBaseConfiguration.create(conf);
		HConnection hbaseConnection = HConnectionManager.createConnection(hbaseConfig);

		initialize(args.collection, fs);

		table = hbaseConnection.getTable(args.table);

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
		ToolRunner.run(new BooleanRetrievalHBase(), args);
	}
}
