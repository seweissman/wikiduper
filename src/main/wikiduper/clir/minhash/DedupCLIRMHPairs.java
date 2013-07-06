package wikiduper.clir.minhash;

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
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.io.array.ArrayListOfLongsWritable;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.io.pair.PairOfStringInt;

public class DedupCLIRMHPairs extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(DedupCLIRMHPairs.class);


    private static class DedupMapper extends MapReduceBase implements
    Mapper<ArrayListOfLongsWritable, ArrayListWritable<IntWritable>, PairOfInts, IntWritable> {
        static final int MAXSIZE = 20;
        static final IntWritable ONE = new IntWritable();
        static PairOfInts pairOut;
        static int nSamples;
        
        public void map(ArrayListOfLongsWritable key, ArrayListWritable<IntWritable> sentenceList, OutputCollector<PairOfInts, IntWritable> output,
                Reporter reporter) throws IOException {
            IntWritable s1;
            IntWritable s2;
            int s1line;
            int s2line;
            for(int i = 0; i < sentenceList.size() && i < MAXSIZE;i++){
                s1 = sentenceList.get(i);
                s1line = s1.get()/nSamples;                
                for(int j=i+1;j < sentenceList.size() && j < MAXSIZE;j++){
                    s2 = sentenceList.get(j);
                    s2line = s2.get()/nSamples;
                    pairOut = new PairOfInts();
                    if(s1line == s2line){
                        continue;
                    }
                    /*
                    if(((s1.get()/nSamples)%2 == 0 && (s2.get()/nSamples)%2 == 0) || 
                            ((s1.get()/nSamples)%2 != 0 && (s2.get()/nSamples)%2 != 0)){
                        continue;
                    }
                    */
                    if(s1.get() < s2.get()){
                        //pairOut.set(s1.get(), s2.get());
                        pairOut.set(s1line, s2line);
                    }else{
                        //pairOut.set(s2.get(), s1.get());
                        pairOut.set(s2line, s1line);
                    }

                    output.collect(pairOut, ONE);
                }
            }
        }
        public void configure(JobConf job) {
            nSamples = job.getInt("nSamples", 100);
            ONE.set(1);
        }
    } 
    
    
    private static class DedupReducer extends MapReduceBase implements
    Reducer<PairOfInts, IntWritable, PairOfInts, IntWritable> {
        
        static final IntWritable COUNT = new IntWritable();
        
        @Override
        public void reduce(PairOfInts pair, Iterator<IntWritable> counts,
                OutputCollector<PairOfInts, IntWritable> output, Reporter arg3) throws IOException {
            int ct = 0;
            while(counts.hasNext()){
                ct += counts.next().get();
            }
            COUNT.set(ct);
            //System.out.println("output: p1:" + p1);
            //System.out.println("output: p2:" + p2);
            output.collect(pair, COUNT);
            
        }
    }  
    
    
    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String nSamplesOption = "M";

    @SuppressWarnings("static-access")
    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("bz2 input path").create(INPUT));
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("integer")
                .hasArg().withDescription("number of samples").create(nSamplesOption));

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();
        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) ||!cmdline.hasOption(nSamplesOption)) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String inputPath = cmdline.getOptionValue(INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);
        String nSamplesIn = cmdline.getOptionValue(nSamplesOption);
        int reduceTasks = 1;

        LOG.info("Tool name: " + this.getClass().getName());
        LOG.info(" - bz2 file: " + inputPath);
        LOG.info(" - output file: " + outputPath);

        JobConf conf = new JobConf(getConf(), DedupCLIRMHPairs.class);
        conf.setJobName(String.format("DedupSentencePairs[%s: %s, %s: %s]", INPUT, inputPath, OUTPUT, outputPath));


        conf.setNumMapTasks(4);
        conf.setNumReduceTasks(reduceTasks);
        
        conf.setInt("nSamples", Integer.parseInt(nSamplesIn));

        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));

        // Set heap space - using old API
        conf.set("mapred.job.map.memory.mb", "2048");
        conf.set("mapred.map.child.java.opts", "-Xmx2048m");
        conf.set("mapred.job.reduce.memory.mb", "4096");
        conf.set("mapred.reduce.child.java.opts", "-Xmx4096m");
        
        conf.setMapperClass(DedupMapper.class);
        conf.setReducerClass(DedupReducer.class);
        
        conf.setInputFormat(SequenceFileInputFormat.class);
        //conf.setOutputFormat(TextOutputFormat.class);
        //conf.setOutputFormat(SequenceFileOutputFormat.class);
        conf.setOutputFormat(MapFileOutputFormat.class);
        
        conf.setMapOutputKeyClass(PairOfInts.class);
        conf.setMapOutputValueClass(IntWritable.class);
        
        conf.setOutputKeyClass(PairOfInts.class);
        conf.setOutputValueClass(IntWritable.class);

        // Delete the output directory if it exists already.
        Path outputDir = new Path(outputPath);
        FileSystem.get(conf).delete(outputDir, true);

        JobClient.runJob(conf);
        
        return 0;
    }

    public DedupCLIRMHPairs() {}

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new DedupCLIRMHPairs(), args);
    }
}
