package wikiduper.utils.junk;

import java.io.IOException;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.pair.PairOfStrings;

public class RemoveKeyLessThan extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(RemoveKeyLessThan.class);
    public static int minsim = 2;
    private static class MyMapper extends Mapper<IntWritable, PairOfStrings, IntWritable, PairOfStrings> {
        @Override
        public void map(IntWritable key, PairOfStrings value, Context context)
                throws IOException, InterruptedException {
            if(key.get() < minsim){
                return;
            }
            context.write(key, value);
        }
        
        @Override
        public void setup(Context context){
            minsim = context.getConfiguration().getInt("minsim", 2);
        }
    }

    /**
     * Creates an instance of this tool.
     */
    public RemoveKeyLessThan() {}

    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String NUM_REDUCERS = "numReducers";
    private static final String MINSIM = "minsim";
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
        
        options.addOption(OptionBuilder.withArgName("num").hasArg()
               .withDescription("minimum similarity to report").create(MINSIM));

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
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String inputPath = cmdline.getOptionValue(INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);

        int minsim = cmdline.hasOption(MINSIM) ? Integer.valueOf(cmdline.getOptionValue(MINSIM)):2;

        LOG.info("Tool: " + RemoveKeyLessThan.class.getSimpleName());
        LOG.info(" - input path: " + inputPath);
        LOG.info(" - output path: " + outputPath);

        // set job configurations
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJobName("SentenceSimilarityCount");
        job.setJarByClass(RemoveKeyLessThan.class);
        job.setNumReduceTasks(0);
        
        conf.setInt("minsim", minsim);
        conf.set("mapred.job.map.memory.mb", "6144");
        conf.set("mapred.map.child.java.opts", "-Xmx6144m");
        conf.set("mapred.job.reduce.memory.mb", "6144");
        conf.set("mapred.reduce.child.java.opts", "-Xmx6144m");

        
        //Job 1
        FileInputFormat.setInputPaths(job, new Path(inputPath));
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
        job.setMapperClass(MyMapper.class);

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
        ToolRunner.run(new RemoveKeyLessThan(), args);
    }
}