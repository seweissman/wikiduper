package wikiduper.analysis;

import java.io.IOException;
import java.util.HashSet;
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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import cern.colt.Arrays;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.pair.PairOfStrings;

public class SentenceSimilarityCount extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(SentenceSimilarityCount.class);


    private static class ClusterReducer extends Reducer<LongWritable, PairOfStrings, LongWritable, ArrayListWritable<Text>> {
        private static final ArrayListWritable<Text> VALUE = new ArrayListWritable<Text>();
        private static final HashSet<String> clusterSentences = new HashSet<String>();
        private static final HashSet<String> clusterDocs = new HashSet<String>();
        @Override
        public void reduce(LongWritable clusterID, Iterable<PairOfStrings> docs, Context context)
                throws IOException, InterruptedException {

            // iterate through all sentences from other wiki articles that have hashed to the same value as one of the sentences in the wiki
            // article denoted by wikiID
            VALUE.clear();
            clusterSentences.clear();
            clusterDocs.clear();
            Iterator<PairOfStrings> iter = docs.iterator();
            PairOfStrings docsentence;
            while (iter.hasNext()) {
                docsentence = iter.next();
                String doc = docsentence.getLeftElement();
                String sentence = docsentence.getRightElement();
                clusterSentences.add(sentence);
                clusterDocs.add(doc);
            }
            double score = TemplateClusters.scoreCluster(clusterSentences);
            if(score < .6){
                for(String doc : clusterDocs){
                    Text docout = new Text();
                    docout.set(doc);
                    VALUE.add(docout);
                }
                context.write(clusterID, VALUE);
            }



            
        }
    }
   
    private static class MyMapper extends Mapper<LongWritable, ArrayListWritable<Text>, PairOfStrings, IntWritable> {
        private static final PairOfStrings KEY = new PairOfStrings();
        private static final IntWritable ONE = new IntWritable(1);
        //private static long threshold;
        @Override
        public void map(LongWritable key, ArrayListWritable<Text> doclist, Context context)
                throws IOException, InterruptedException {

            //System.out.println(sentences.toString());
            Text doc1;
            Text doc2;
            if(doclist.size() > 10000)
                return;
            for (int i=0;i<doclist.size();i++){
                doc1 = doclist.get(i);
                for(int j=i+1;j<doclist.size();j++){
                    doc2 = doclist.get(j);
                    if (doc1.compareTo(doc2) != 0) {
                        KEY.set(doc1.toString(),doc2.toString());
                        context.write(KEY,  ONE);
                    }
                }
            }
        }
        
        //public void configure(JobConf job){
          //  threshold = job.getLong("threshold",1000);            
            
        //}
    }

    private static class MyReducer extends Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
        private static final IntWritable SUM = new IntWritable();

        
        @Override
        public void reduce(PairOfStrings articlepair, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            // iterate through all sentences from other wiki articles that have hashed to the same value as one of the sentences in the wiki
            // article denoted by wikiID
            Iterator<IntWritable> iter = values.iterator();
            int sum = 0;
            while (iter.hasNext()) {
                sum+= iter.next().get();
            }
            
            SUM.set(sum);
            context.write(articlepair, SUM);

        }
   }

    private static class FlipMapper extends Mapper<PairOfStrings, IntWritable, IntWritable, PairOfStrings> {
        @Override
        public void map(PairOfStrings docpair, IntWritable sum, Context context)
                throws IOException, InterruptedException {
            if(sum.get() > 1){
                context.write(sum,  docpair);
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
    //private static final String THRESHOLD = "threshold";

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
        
        //options.addOption(OptionBuilder.withArgName("num").hasArg()
          //      .withDescription("threshold value").create(THRESHOLD));

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
        //long threshold = Long.valueOf(cmdline.getOptionValue(THRESHOLD));
        String tmpPath = "tmppath";
        String tmpPath2 = "tmppath2";
        int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 20;
        

        LOG.info("Tool: " + SentenceSimilarityCount.class.getSimpleName());
        LOG.info(" - input path: " + inputPath);
        LOG.info(" - output path: " + outputPath);
        LOG.info(" - number of reducers: " + reduceTasks);

        // set job configurations
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJobName("SentenceSimilarityCount");
        job.setJarByClass(SentenceSimilarityCount.class);
        job.setNumReduceTasks(reduceTasks);
        
        //conf.setLong("threshold", threshold);
        conf.set("mapred.job.map.memory.mb", "6144");
        conf.set("mapred.map.child.java.opts", "-Xmx6144m");
        conf.set("mapred.job.reduce.memory.mb", "6144");
        conf.set("mapred.reduce.child.java.opts", "-Xmx6144m");

        
        //Job 1
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(tmpPath));

        // set input/output format of the job
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);

        // set output key/value data types
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(PairOfStrings.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(ArrayListWritable.class);

        // define Mapper and Reducer
        job.setReducerClass(ClusterReducer.class);
        
        
        // Delete the output directory if it exists already.
        Path outputDir = new Path(tmpPath);
        FileSystem.get(conf).delete(outputDir, true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
        
        // Job 2 
        job = Job.getInstance(conf);
        
        job.setJobName("SentenceSimilarityCount-2");
        job.setJarByClass(SentenceSimilarityCount.class);
        job.setNumReduceTasks(reduceTasks);

        FileInputFormat.setInputPaths(job, new Path(tmpPath));
        FileOutputFormat.setOutputPath(job, new Path(tmpPath2));

        // set input/output format of the job
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);

        // set output key/value data types
        job.setMapOutputKeyClass(PairOfStrings.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(PairOfStrings.class);
        job.setOutputValueClass(IntWritable.class);

        // define Mapper and Reducer
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setCombinerClass(MyReducer.class);
        
        // Delete the output directory if it exists already.
        outputDir = new Path(tmpPath2);
        FileSystem.get(conf).delete(outputDir, true);

        startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
        
        // Job 3
        job = Job.getInstance(conf);
        
        job.setJobName("SentenceSimilarityCount-3");
        job.setJarByClass(SentenceSimilarityCount.class);
        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, new Path(tmpPath2));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // set input/output format of the job
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);

        // set output key/value data types
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PairOfStrings.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(PairOfStrings.class);

        // define Mapper and Reducer
        job.setMapperClass(FlipMapper.class);
        
        // Delete the output directory if it exists already.
        outputDir = new Path(outputPath);
        FileSystem.get(conf).delete(outputDir, true);

        startTime = System.currentTimeMillis();
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