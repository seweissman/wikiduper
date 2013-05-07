package analysis;

import java.io.IOException;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import cern.colt.Arrays;
import edu.umd.cloud9.io.pair.PairOfInts;


public class DedupSentenceCounts extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(DedupSentenceCounts.class);

    // Emit the article pair where the "left" article always has the smallest article ID
    // This will make the output unique
    private static class MyMapper extends Mapper<PairOfInts, IntWritable, PairOfInts, IntWritable> {
        private static final PairOfInts KEY = new PairOfInts();

        @Override
        public void map(PairOfInts key, IntWritable count, Context context)
                throws IOException, InterruptedException {
            
            if (key.getLeftElement() > key.getRightElement()) {
                KEY.set(key.getRightElement(), key.getLeftElement());
            }
            context.write(KEY,  count);
        }
    }

    
    // emit only one copy of the count results for each unique article pair
    private static class MyReducer extends Reducer<PairOfInts, IntWritable, PairOfInts, IntWritable> {
        
        private static final IntWritable VALUE = new IntWritable(0);
        
        @Override
        public void reduce(PairOfInts articlePair, Iterable<IntWritable> counts, Context context)
                throws IOException, InterruptedException {
            
            Iterator<IntWritable> iter = counts.iterator();
            int max = 0;
            while (iter.hasNext()) {
                int tmp = iter.next().get();
                if (tmp > max)
                    max = tmp;
            }
            
            VALUE.set(max);
            context.write(articlePair, VALUE);
        }
   }




    /**
     * Creates an instance of this tool.
     */
    public DedupSentenceCounts() {}

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
        int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 10;
        

        LOG.info("Tool: " + DedupSentenceCounts.class.getSimpleName());
        LOG.info(" - input path: " + inputPath);
        LOG.info(" - output path: " + outputPath);
        LOG.info(" - number of reducers: " + reduceTasks);

        // set job configurations
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJobName("DedupSentenceCounts");
        job.setJarByClass(DedupSentenceCounts.class);
        job.setNumReduceTasks(reduceTasks);
        
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // set input/output format of the job
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // set output key/value data types
        job.setMapOutputKeyClass(PairOfInts.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(PairOfInts.class);
        job.setOutputValueClass(IntWritable.class);

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
        ToolRunner.run(new DedupSentenceCounts(), args);
    }
}