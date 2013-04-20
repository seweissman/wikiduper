/*
 * Cloud9: A MapReduce Library for Hadoop
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */



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
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import courseproj.hash.MultiplyShiftHash;
import edu.umd.cloud9.collection.wikipedia.*;
import edu.umd.cloud9.io.array.ArrayListOfLongsWritable;
import edu.umd.cloud9.io.pair.PairOfLongInt;

public class MinhashWikipediaPages extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(MinhashWikipediaPages.class);
  
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
   *   * implement multiple minhash values returned per sentence (m groups of n hashes)
   *   * other shingling granularities?
   *   * Write a class to extract results
   * 
   */

  private static enum PageTypes {
    TOTAL, REDIRECT, DISAMBIGUATION, EMPTY, ARTICLE, STUB, NON_ARTICLE
  };

  private static class SentenceMapperRegex extends MapReduceBase implements
      Mapper<LongWritable, WikipediaPage, ArrayListOfLongsWritable, PairOfLongInt> {
	  
	    static long rseed;
	    static long seeds[];
	    static int NHASH;
	    static int NHASHOUTPUTBITS;
	    static int MINLEN;
	    static MultiplyShiftHash hashfamily;

	    // The minhash signature
	    static final ArrayListOfLongsWritable SIG = new ArrayListOfLongsWritable(NHASH);
	    
	    // The document-sentence identifier
	    static final PairOfLongInt DOCSENT = new PairOfLongInt();
	    
	    // seed list could be produced in job and passed as message
	    static{
	      seeds = new long[NHASH];
	      Random r = new Random(rseed);
	      int ct = 0;
	      while(ct < NHASH){
	        seeds[ct] = r.nextLong();
	        ct++;
	      }
	      hashfamily = new MultiplyShiftHash(NHASHOUTPUTBITS,seeds);
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
	    

    public void map(LongWritable key, WikipediaPage p, OutputCollector<ArrayListOfLongsWritable, PairOfLongInt> output,
        Reporter reporter) throws IOException {
      reporter.incrCounter(PageTypes.TOTAL, 1);

      if (p.isRedirect()) 
      {
        reporter.incrCounter(PageTypes.REDIRECT, 1);
      } 
      else if (p.isDisambiguation()) 
      {
        reporter.incrCounter(PageTypes.DISAMBIGUATION, 1);
      } 
      else if (p.isEmpty()) 
      {
        reporter.incrCounter(PageTypes.EMPTY, 1);
      } 
      else if (p.isArticle()) 
      {
        reporter.incrCounter(PageTypes.ARTICLE, 1);

        if (p.isStub()) 
        {
          reporter.incrCounter(PageTypes.STUB, 1);
        }
        
        String line = p.getContent();
        Matcher m = sentenceregex.matcher(line);

        // Assume each doc is on its own line; track sentence number by counting
        int sentencect = 0;
        
        // For each sentence in the input text:
        while(m.find()){
          // Initialize the minhash vector
          for(int i=0;i<NHASH;i++){
            SIG.set(i, Long.MAX_VALUE);
          }
          String sentence = m.group(1);
          //System.out.println("Sentence: " + sentence);
          
          // Shingle sentence by word
          StringTokenizer itr = new StringTokenizer(sentence);
          int wordct = 0;
          // Calculate hash vector for each shingle
          while (itr.hasMoreTokens()) {
            String word = itr.nextToken();
            long hashes[] = hashfamily.hash(word);
            // Update the minhash signature
            for(int j=0;j<hashes.length;j++){
              if(hashes[j] < SIG.get(j)){
                SIG.set(j, hashes[j]);
              }
              //System.out.println("word: " + word + " " + hashes[j]);
            }
            // Keep track of the word ct to avoid short sentences
            wordct++;
          }
          
          //for(int i=0;i<NHASH;i++){
            //System.out.println("minhash " + i + "= " + SIG.get(i));
          //}
          //System.out.println("SIG size = " + SIG.size());

          // If the sentence meads min word ct requirements, emit the signature and the sentence/doc ID
          if(wordct > MINLEN){
            DOCSENT.set(key.get(), sentencect);
//            context.write(SIG, DOCSENT);
            output.collect(SIG, DOCSENT);
          }
          sentencect++;
        }
        
      } 
      else 
      {
        reporter.incrCounter(PageTypes.NON_ARTICLE, 1);
      }
    }
    
    public void configure(JobConf job) {
        rseed = Long.parseLong(job.get("rseed"));
        NHASH = Integer.parseInt(job.get("NHASH"));
        NHASHOUTPUTBITS = Integer.parseInt(job.get("NHASHOUTPUTBITS"));
        MINLEN = Integer.parseInt(job.get("MINLEN"));
        
	    for(int i=0; i<NHASH; i++)
	    {
	    	SIG.add(0);
		}
    }
  }
  
  private static class GroupReducer extends MapReduceBase implements Reducer<ArrayListOfLongsWritable, PairOfLongInt, ArrayListOfLongsWritable, PairOfLongInt> {

	  public void reduce(ArrayListOfLongsWritable key, Iterator<PairOfLongInt> values, OutputCollector<ArrayListOfLongsWritable, PairOfLongInt> output, Reporter reporter) 
			  throws IOException {

		  boolean gt1 = false;
		  
		  while (values.hasNext())
		  {
			  PairOfLongInt val = values.next();
		      if(values.hasNext()) 
		    	  gt1 = true;
		      if(gt1) 
		    	  output.collect(key, val);
		  }
	  }
  }

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String LANGUAGE_OPTION = "wiki_language";
  
  @SuppressWarnings("static-access")
  @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path")
        .hasArg().withDescription("bz2 input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path")
            .hasArg().withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("en|sv|de|cs|es|zh|ar|tr").hasArg()
        .withDescription("two-letter language code").create(LANGUAGE_OPTION));
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
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }
    
    String language = null;
    if (cmdline.hasOption(LANGUAGE_OPTION)) {
      language = cmdline.getOptionValue(LANGUAGE_OPTION);
      if(language.length()!=2){
        System.err.println("Error: \"" + language + "\" unknown language!");
        return -1;
      }
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    //int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ?
      //  Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

    LOG.info("Tool name: " + this.getClass().getName());
    LOG.info(" - bz2 file: " + inputPath);
    LOG.info(" - output file: " + outputPath);
    LOG.info(" - language: " + language);
    
    JobConf conf = new JobConf(getConf(), MinhashWikipediaPages.class);
    conf.setJobName(String.format("MinhashWikipediaPages[%s: %s, %s: %s, %s: %s]", INPUT, inputPath, OUTPUT, outputPath, LANGUAGE_OPTION, language));
    
    conf.set("rseed", "1123456");
    conf.set("NHASH", "10");
    conf.set("NHASHOUTPUTBITS", "10");
    conf.set("MINLEN", "5");

    conf.setNumMapTasks(10);
    conf.setNumReduceTasks(0);

    FileInputFormat.setInputPaths(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    if(language != null){
      conf.set("wiki.language", language);
    }
    
    conf.setInputFormat(WikipediaPageInputFormat.class);
    conf.setOutputFormat(NullOutputFormat.class);
    
    conf.setOutputKeyClass(ArrayListOfLongsWritable.class);
    conf.setOutputValueClass(PairOfLongInt.class);

    conf.setMapperClass(SentenceMapperRegex.class);
    conf.setReducerClass(GroupReducer.class);
    conf.setNumReduceTasks(1);
    
    // Delete the output directory if it exists already.
    Path outputDir = new Path(outputPath);
    FileSystem.get(conf).delete(outputDir, true);

    JobClient.runJob(conf);

    return 0;
  }

  public MinhashWikipediaPages() {}

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new MinhashWikipediaPages(), args);
  }
}
