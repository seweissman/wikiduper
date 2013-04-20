import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

import courseproj.hash.MultiplyShiftHash;

import cern.colt.Arrays;
import edu.umd.cloud9.io.array.ArrayListOfLongsWritable;
import edu.umd.cloud9.io.pair.PairOfLongInt;

public class MinHash extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(MinHash.class);

  /* SentenceMapperRegex
   * 
   * Parameters that can be tweaked: NHASH, NHASHOUTPUTBITS, MINLEN
   * 
   * Pulls out sentences from text input using a regex. 
   * Emits one NHASH-length minhash signature per sentence.
   * Each hash is NHASHOUTPUTBITS long. (So signature is NHASH*NHASHOUTPUTBITS long.)
   * Sentences are shingled by individual words. 
   * If sentences are less than MINLEN words, then they are skipped.
   * 
   * 
   * Output values are (offset,nsentence) where offset is the byte offset of the input line in the
   * input text and nsentence is the number of the sentence in the line. (starting from 0)
   * 
   * TODO:
   *   * Read initializing info (i.e. initial hash seeds) from Job parameters instead of hard coded.
   *   * Test sentence parsing to see if regex needs tweaking.
   *   * implement k at a time handling of sentences
   *   * Write a class to extract results
   * 
   */
  //private static class SentenceMapperRegex extends Mapper<LongWritable, Text, ArrayListOfLongsWritable, PairOfLongInt> {
  private static class SentenceMapperRegex extends Mapper<LongWritable, Text, ArrayListOfLongsWritable, Text> {
    static long rseed = 1123456;
    static long seeds[];
    static long sigseed; // Seed to use when randoly selecting signature vectors
    static long MINHASH[];
    
    static int NHASH = 20; // Total number of hashes per sentence
    static int K = 8; // Length of hash vector
    static int N = 5; // Number of hashes per input sentence (N < NHASH)
    static int NHASHOUTPUTBITS = 30;
    static int SHINGLELEN = 15;
    static int MINLEN = 20;
    //static int NSENTENCE = 3; // Number of sentences to match at a time
    static MultiplyShiftHash hashfamily;

    // The minhash signature
    static final ArrayListOfLongsWritable SIG = new ArrayListOfLongsWritable(K);
    
    // The document-sentence identifier
    static final PairOfLongInt DOCSENT = new PairOfLongInt();
    // for testing
    static final Text SENTENCE = new Text();
    
    // seed list could be produced in job and passed as message
    static{
      seeds = new long[NHASH];
      Random r = new Random(rseed);
      int ct = 0;
      while(ct < NHASH){
        seeds[ct] = r.nextLong();
        ct++;
      }
      sigseed = r.nextLong();
      hashfamily = new MultiplyShiftHash(NHASHOUTPUTBITS,seeds);
      MINHASH = new long[NHASH];
    }
    
    @Override 
    public void setup(Context context){
      for(int i=0; i<K; i++){
          SIG.add(0);
      }
    }
    
    //Adapted from http://stackoverflow.com/questions/5553410/regular-expression-match-a-sentence
    static final Pattern sentenceregex = Pattern.compile(
        "# Match a sentence ending in punctuation or EOS.\n" +
        "[\\s]*    # Leading white space\n" + 
        "([A-Z\"]    # First char capital letter or quotation\n" +
        "[^.!?]*      # Greedily consume up to punctuation.\n" +
        "(?:          # Group for unrolling the loop.\n" +
        "  [.!?]      # (special) inner punctuation ok if\n" +
        "  (?!['\"]?\\s|$)  # not followed by ws or EOS.\n" +
        "  [^.!?]*    # Greedily consume up to punctuation.\n" +
        ")*           # Zero or more (special normal*)\n" +
        "[.!?]?       # Optional ending punctuation.\n" +
        "['\"]?)       # Optional closing quote.\n" +
        "\\s*$?       # Trailing white space\n",
        Pattern.MULTILINE | Pattern.COMMENTS);

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = ((Text) value).toString();
      Matcher m = sentenceregex.matcher(line);

      // Assume each doc is on its own line; track sentence number by counting
      int sentencect = 0;

      // For each sentence in the input text:
      while(m.find()){
        // Initialize the minhash vector
        for(int i=0;i<NHASH;i++){
          MINHASH[i] = Long.MAX_VALUE;
        }
        String sentence = m.group(1);
        //System.out.println("Sentence: " + sentence);

        
        int shinglect = 0;
        // Calculate hash vector for each shingle
        String hashval[] = new String[seeds.length];
        // skip sentences that are too short
        if(sentence.length() < SHINGLELEN) return;
        for(int i=0;i<sentence.length() - SHINGLELEN + 1; i++){
            String shingle = sentence.substring(i, i+SHINGLELEN);
            long hash[] = hashfamily.hash(shingle);
            // Update the minhash signature
            for(int j=0;j<hash.length;j++){
                if(hash[j] < MINHASH[j]){
                    MINHASH[j] = hash[j];
                    hashval[j] = shingle;
                }
            //System.out.println("word: " + word + " " + hashes[j]);
            }
            // Keep track of the word ct to avoid short sentences
            shinglect++;
        }
        
        /*
        for(int i=0;i<MINHASH.length;i++){
            System.out.print(MINHASH[i] + " ");
        }
        System.out.println();
        for(int i=0;i<hashval.length;i++){
            System.out.print(hashval[i] + " ");
        }
        System.out.println();
        */

        // If the sentence meads min shingle ct requirements, emit the signature and the sentence/doc ID
        if(shinglect > MINLEN){
            DOCSENT.set(key.get(), sentencect);
            SENTENCE.set(sentence + " " + key.get() + ":" + sentencect);
          
          // generate N k-minhash-signatures
          // start from same seed, otherwise doesn't work so well
          Random r = new Random(sigseed);
          for(int j=0; j<N; j++){
              for(int i=0; i<K; i++){
                  int x = r.nextInt(NHASH);
                  SIG.set(i, MINHASH[x]);
              }
              //context.write(SIG, DOCSENT);
              context.write(SIG, SENTENCE);
          }
          
        }
        

        sentencect++;
      }
    }
  }


  
  /**
   * Emits groups of sentences that hash to the same value. Only emits if there is more than one value for the key. 
   *
   */
  //private static class GroupReducer extends Reducer<ArrayListOfLongsWritable, PairOfLongInt, ArrayListOfLongsWritable, PairOfLongInt> {
  private static class GroupReducer extends Reducer<ArrayListOfLongsWritable, Text, ArrayListOfLongsWritable, Text> {
/*
    @Override
    public void reduce(ArrayListOfLongsWritable key, Iterable<PairOfLongInt> values, Context context)
            throws IOException, InterruptedException {
      Iterator<PairOfLongInt> iter = values.iterator();
      
      boolean gt1 = false;
      
      while (iter.hasNext()) {
        PairOfLongInt val = iter.next();
        if(iter.hasNext()) gt1 = true;
        if(gt1) context.write(key, val);
      }
      
    }
    */
      @Override
     public void reduce(ArrayListOfLongsWritable key, Iterable<Text> values, Context context)
                  throws IOException, InterruptedException {
        Iterator<Text> iter = values.iterator();
        
        boolean gt1 = false;
        
        while (iter.hasNext()) {
          Text val = iter.next();
          if(iter.hasNext()) gt1 = true;
          if(gt1) context.write(key, val);
        }
        
      }
  }

  /**
   * Creates an instance of this tool.
   */
  public MinHash() {}

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  //private static final String NUM_REDUCERS = "numReducers";`

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
    //options.addOption(OptionBuilder.withArgName("num").hasArg()
      //  .withDescription("number of reducers").create(NUM_REDUCERS));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    //int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ?
      //  Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

    LOG.info("Tool: " + MinHash.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    //LOG.info(" - number of reducers: " + reduceTasks);

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);
    job.setJobName(MinHash.class.getSimpleName());
    job.setJarByClass(MinHash.class);

    //job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setOutputKeyClass(ArrayListOfLongsWritable.class);
    //job.setOutputValueClass(PairOfLongInt.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(SentenceMapperRegex.class);
    //job.setCombinerClass(MyReducer.class);
    job.setReducerClass(GroupReducer.class);
    job.setNumReduceTasks(1);
    
    // Delete the output directory if it exists already.
    Path outputDir = new Path(outputPath);
    FileSystem.get(conf).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new MinHash(), args);
  }
  
  

  
}