package analysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import cern.colt.Arrays;
import edu.umd.cloud9.io.array.ArrayListOfLongsWritable;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.io.pair.PairOfStringInt;
import edu.umd.cloud9.util.fd.Object2IntFrequencyDistribution;
import edu.umd.cloud9.util.fd.Object2IntFrequencyDistributionEntry;


public class SentenceSimilarityCount extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(SentenceSimilarityCount.class);
    
    
    // Mapper: emits (term, tf) for every word in the document.
    private static class MyMapper extends Mapper<ArrayListOfLongsWritable, ArrayListWritable<PairOfStringInt>, IntWritable, PairOfInts> {
        private static final IntWritable KEY = new IntWritable();
        private static final PairOfInts VALUE = new PairOfInts();

        @Override
        public void map(ArrayListOfLongsWritable hashSignature, ArrayListWritable<PairOfStringInt> sentences, Context context)
                throws IOException, InterruptedException {

            
            // for each sentence, emit all other sentences
            for (PairOfStringInt sentenceID : sentences) {
                int wikiArticleID = Integer.parseInt(sentenceID.getLeftElement());
                
                for (PairOfStringInt otherSentence : sentences) {
                    
                    if (otherSentence.compareTo(sentenceID) != 0) {
                        KEY.set(wikiArticleID);
                        VALUE.set(Integer.parseInt(otherSentence.getLeftElement()), otherSentence.getRightElement());
                        
                        context.write(KEY,  VALUE);
                    }
                }
            }
        }
    }
    
    
    private static class MyReducer extends Reducer<IntWritable, PairOfInts, PairOfInts, IntWritable> {
        private static final PairOfInts KEY = new PairOfInts();
        private static final IntWritable VALUE = new IntWritable();
        
        private static final Object2IntFrequencyDistribution<Integer> COUNTS = new Object2IntFrequencyDistributionEntry<Integer>();
        private static final HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
        private static final Collection<PairOfInts> collection = new ArrayList<PairOfInts>();
        
        @Override
        public void reduce(IntWritable wikiID, Iterable<PairOfInts> values, Context context)
                throws IOException, InterruptedException {
            
            // iterate through all sentences from other wiki articles that have hashed to the same value as one of the sentences in the wiki
            // article denoted by wikiID
            Iterator<PairOfInts> iter = values.iterator();
            while (iter.hasNext()) {
                PairOfInts sentID = iter.next();
                if (!collection.contains(sentID)) {
                    collection.add(sentID);
                    
                    // count the number of sentences from a particular article that have been hashed to the same value
                    // as a sentence in the wiki article with id = wikiID
                    COUNTS.increment(sentID.getLeftElement());
                }
            }
            
            for (Integer otherWikiArticle: COUNTS.keySet()) {
                KEY.set(wikiID.get(), otherWikiArticle);
                VALUE.set(COUNTS.get(otherWikiArticle));
                
                context.write(KEY, VALUE);
            }
            
        }
    }
    
    
    
    
    /**
     * Creates an instance of this tool.
     */
    public SentenceSimilarityCount() {}

    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String NUM_REDUCERS = "numReducers";

    /**
     * Runs this tool.
     */
    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception {
        // add command line arguments
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("input path").create(INPUT));
        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("number of reducers").create(NUM_REDUCERS));

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();

        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }
        
        // check for command line arguments
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
        int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

        LOG.info("Tool: " + SentenceSimilarityCount.class.getSimpleName());
        LOG.info(" - input path: " + inputPath);
        LOG.info(" - output path: " + outputPath);
        LOG.info(" - number of reducers: " + reduceTasks);
        
        // set job configurations
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJobName(SentenceSimilarityCount.class.toString());
        job.setJarByClass(SentenceSimilarityCount.class);
        job.setNumReduceTasks(reduceTasks);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        // set input/output format of the job
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        // set output key/value data types
        job.setMapOutputKeyClass(PairOfStringInt.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        
        // define Mapper and Reducer
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

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
        ToolRunner.run(new SentenceSimilarityCount(), args);
    }
}