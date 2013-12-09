package wikiduper.utils;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;

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
import org.apache.hadoop.hdfs.tools.GetConf;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.pair.PairOfLongInt;
import edu.umd.cloud9.io.pair.PairOfStrings;

public class SampleSentences extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(SampleSentences.class);

    private static class SampleMapper extends MapReduceBase implements
    Mapper<PairOfLongInt, PairOfStrings, PairOfLongInt, PairOfStrings> {

        static long rseed;
        static Random r;
        static String sampleLang;
        static long limit;
        static long count;

        public void map(PairOfLongInt key, PairOfStrings p, OutputCollector<PairOfLongInt, PairOfStrings> output,
                    Reporter reporter) throws IOException {
            
            String lang = p.getLeftElement();
            if(lang.equals(sampleLang)){
                if(Math.abs(r.nextLong())%count <= limit){
                    output.collect(key, p);
                }
                
            }
        }
        

        public void configure(JobConf job) {
            rseed = job.getLong("rseed", 112345);
            r = new Random(rseed);    
            count = job.getLong("count", 1000000);
            int nSamples = job.getInt("nSamples", 1000000);
            if(nSamples > count){
                limit = 1;
            }
            limit = count/nSamples;
        }

        
    }

    
    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String SAMPLE_LANG = "lang";    
    private static final String SAMPLES = "samples";    
    @SuppressWarnings("static-access")
    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("input").create(INPUT));
        
        options.addOption(OptionBuilder.withArgName("lang")
                .hasArg().withDescription("language to sample from").create(SAMPLE_LANG));
        options.addOption(OptionBuilder.withArgName("int")
                .hasArg().withDescription("number of samples").create(SAMPLES));
        
        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();
        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(OUTPUT) || !cmdline.hasOption(INPUT) 
                || !cmdline.hasOption(SAMPLE_LANG) || !cmdline.hasOption(SAMPLES)
                ) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String inputPath = cmdline.getOptionValue(INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);

        String sampleLang = cmdline.getOptionValue(SAMPLE_LANG);
        int nSamples = Integer.parseInt(cmdline.getOptionValue(SAMPLES));
                
        
        LOG.info("Tool name: " + this.getClass().getName());
        LOG.info(" - nput file: " + inputPath);
        LOG.info(" - output file: " + outputPath);
        LOG.info(" - sample language: " + sampleLang);

        JobConf conf = new JobConf(getConf(), SampleSentences.class);

        // Set heap space - using old API
        conf.set("mapred.job.map.memory.mb", "2048");
        conf.set("mapred.map.child.java.opts", "-Xmx2048m");
        conf.set("mapred.job.reduce.memory.mb", "8000");
        conf.set("mapred.reduce.child.java.opts", "-Xmx8000m");
        //conf.set("mapred.child.java.opts", "-Xmx2048m");

        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        
        conf.setOutputKeyClass(PairOfLongInt.class);
        conf.setOutputValueClass(PairOfStrings.class);

        // Job 1
        conf.setJobName(String.format("SampleSentences-2[%s: %s]", OUTPUT, outputPath));

        conf.setNumMapTasks(20);
        conf.setNumReduceTasks(0);
        
        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path("tmppath"));


        // Delete the output directory if it exists already.
        Path outputDir = new Path("tmppath");
        FileSystem.get(conf).delete(outputDir, true);
        
        RunningJob job = JobClient.runJob(conf);

        Counters counters = job.getCounters();
        long count = counters.getCounter(org.apache.hadoop.mapred.Task.Counter.MAP_INPUT_RECORDS);
        LOG.info(" Count from job 1 = " + count);
        
        // Job 2

        conf.setLong("rseed", 1123456);
        conf.setLong("count", count);
        conf.setInt("nSamples", nSamples);
        conf.set("sampleLang", sampleLang);
        
        conf.setNumMapTasks(1);
        conf.setNumReduceTasks(0);
        
        conf.setMapperClass(SampleMapper.class);
        //conf.setReducerClass(SignatureReducer.class);

        conf.setJobName(String.format("SampleSentences-2[%s: %s]", OUTPUT, outputPath));

        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));

        // Delete the output directory if it exists already.
        outputDir = new Path(outputPath);
        FileSystem.get(conf).delete(outputDir, true);
        
        JobClient.runJob(conf);

        return 0;
    }

    public SampleSentences() {}

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new SampleSentences(), args);
    }
}
