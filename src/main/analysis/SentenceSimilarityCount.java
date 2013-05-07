package analysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.pig.parser.AliasMasker.keyvalue_return;

import cern.colt.Arrays;
import edu.umd.cloud9.io.array.ArrayListOfLongsWritable;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.io.pair.PairOfStringInt;
import edu.umd.cloud9.util.fd.Object2IntFrequencyDistribution;
import edu.umd.cloud9.util.fd.Object2IntFrequencyDistributionEntry;


public class SentenceSimilarityCount extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(SentenceSimilarityCount.class);


    private static class MyMapper extends Mapper<ArrayListOfLongsWritable, ArrayListWritable<PairOfStringInt>, IntWritable, PairOfInts> {
        private static final IntWritable KEY = new IntWritable();
        private static final PairOfInts VALUE = new PairOfInts();

        @Override
        public void map(ArrayListOfLongsWritable key, ArrayListWritable<PairOfStringInt> sentences, Context context)
                throws IOException, InterruptedException {

            //System.out.println(sentences.toString());

            // for each sentence, emit all other sentences
            for (PairOfStringInt sentenceID : sentences) {
                int wikiArticleID = Integer.parseInt(sentenceID.getLeftElement());
                //System.out.println("\tKEY: " + wikiArticleID);

                KEY.set(wikiArticleID);
                for (PairOfStringInt otherSentence : sentences) {
                    if (otherSentence.compareTo(sentenceID) != 0) {
                        VALUE.set(Integer.parseInt(otherSentence.getLeftElement()), otherSentence.getRightElement());

                        //System.out.println("\t\tVALUE: " + VALUE.toString());
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
        public static final Map<PairOfInts, Integer> collection = new HashMap<PairOfInts, Integer>();
        private int threshold;

        @Override
        public void reduce(IntWritable wikiID, Iterable<PairOfInts> values, Context context)
                throws IOException, InterruptedException {

            // iterate through all sentences from other wiki articles that have hashed to the same value as one of the sentences in the wiki
            // article denoted by wikiID
            threshold = context.getConfiguration().getInt("threshold", 40);
            Iterator<PairOfInts> iter = values.iterator();

            while (iter.hasNext()) {
                PairOfInts sentID = iter.next();
                // make sure we don't count the same exact sentence twice!
                if (!collection.containsKey(sentID) && sentID.getLeftElement() != wikiID.get()) {
                    collection.put(sentID,  1);

                    // count the number of sentences from a particular article that have been hashed to the same value
                    // as a sentence in the wiki article with id = wikiID
                    if (COUNTS.contains(sentID.getLeftElement())) {
                        COUNTS.increment(sentID.getLeftElement());
                    }
                    else {
                        COUNTS.set(sentID.getLeftElement(), 1);
                    }

                }
            }

            System.out.println("Wiki Article: " + wikiID.get());
            for (Integer otherWikiArticle: COUNTS.keySet()) {

                // only output if the article pair has more than a threshold number of "similar" sentences in common
                if (COUNTS.get(otherWikiArticle) > threshold) {
                    KEY.set(wikiID.get(), otherWikiArticle);
                    VALUE.set(COUNTS.get(otherWikiArticle));

                    System.out.println("\t" + otherWikiArticle + " " + VALUE.get());

                    context.write(KEY, VALUE);
                }
            }

            collection.clear();
            COUNTS.clear();
        }
   }




    /**
     * Creates an instance of this tool.
     */
    public SentenceSimilarityCount() {}

    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String NUM_REDUCERS = "numReducers";
    private static final String THRESHOLD = "threshold";

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
        
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("threshold value").create(THRESHOLD));

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
        int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 6;
        int threshold = cmdline.hasOption(THRESHOLD) ? Integer.parseInt(cmdline.getOptionValue(THRESHOLD)) : 40;
        

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
        
        conf.setInt("THRESHOLD", threshold);
        
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // set input/output format of the job
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // set output key/value data types
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PairOfInts.class);
        job.setOutputKeyClass(PairOfInts.class);
        job.setOutputValueClass(IntWritable.class);

        // define Mapper and Reducer
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        
        conf.set("mapred.job.map.memory.mb", "6144");
        conf.set("mapred.map.child.java.opts", "-Xmx6144m");
        conf.set("mapred.job.reduce.memory.mb", "6144");
        conf.set("mapred.reduce.child.java.opts", "-Xmx6144m");
        
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