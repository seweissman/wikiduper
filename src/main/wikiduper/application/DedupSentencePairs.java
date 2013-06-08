package wikiduper.application;

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
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.io.array.ArrayListOfLongsWritable;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.io.pair.PairOfStringInt;

public class DedupSentencePairs extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(DedupSentencePairs.class);


    private static class DedupMapper extends MapReduceBase implements
    Mapper<ArrayListOfLongsWritable, ArrayListWritable<PairOfStringInt>, ArrayListOfIntsWritable, IntWritable> {
        static final int MAXSIZE = 20;
        static final IntWritable ONE = new IntWritable();
        static final ArrayListOfIntsWritable sentences = new ArrayListOfIntsWritable();
        public void map(ArrayListOfLongsWritable key, ArrayListWritable<PairOfStringInt> sentenceList, OutputCollector<ArrayListOfIntsWritable, IntWritable> output,
                Reporter reporter) throws IOException {
            PairOfStringInt p1;
            PairOfStringInt p2;
            for(int i = 0; i < sentenceList.size() && i < MAXSIZE;i++){
                p1 = sentenceList.get(i);
                for(int j=i+1;j < sentenceList.size() && j < MAXSIZE;j++){
                    p2 = sentenceList.get(j);
                    if(p1.compareTo(p2) < 0){
                        sentences.add(Integer.parseInt(p1.getLeftElement()));
                        sentences.add(p1.getRightElement());
                        sentences.add(Integer.parseInt(p2.getLeftElement()));
                        sentences.add(p2.getRightElement());
                    }else{
                        sentences.add(Integer.parseInt(p2.getLeftElement()));
                        sentences.add(p2.getRightElement());
                        sentences.add(Integer.parseInt(p1.getLeftElement()));
                        sentences.add(p1.getRightElement());
                    }

                    output.collect(sentences, ONE);
                    sentences.clear();
                }
            }
        }
        public void configure(JobConf job) {
            ONE.set(1);
        }
    } 
    
    private static class DedupReducer extends MapReduceBase implements
    Reducer<ArrayListOfIntsWritable, IntWritable, PairOfInts, PairOfInts> {
        
        PairOfInts p1 = new PairOfInts();
        PairOfInts p2 = new PairOfInts();
        
        @Override
        public void reduce(ArrayListOfIntsWritable pair, Iterator<IntWritable> counts,
                OutputCollector<PairOfInts, PairOfInts> output, Reporter arg3) throws IOException {
            p1.set(pair.get(0), pair.get(1));
            p2.set(pair.get(2), pair.get(3));
            //System.out.println("output: p1:" + p1);
            //System.out.println("output: p2:" + p2);
            output.collect(p1, p2);
            
        }
    }  
    

    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String NUM_REDUCERS = "numReducers";

    @SuppressWarnings("static-access")
    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("bz2 input path").create(INPUT));
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("output path").create(OUTPUT));
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

        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String inputPath = cmdline.getOptionValue(INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);
        int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

        LOG.info("Tool name: " + this.getClass().getName());
        LOG.info(" - bz2 file: " + inputPath);
        LOG.info(" - output file: " + outputPath);

        JobConf conf = new JobConf(getConf(), DedupSentencePairs.class);
        conf.setJobName(String.format("DedupSentencePairs[%s: %s, %s: %s]", INPUT, inputPath, OUTPUT, outputPath));


        conf.setNumMapTasks(4);
        conf.setNumReduceTasks(reduceTasks);

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
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        
        conf.setMapOutputKeyClass(ArrayListOfIntsWritable.class);
        conf.setMapOutputValueClass(IntWritable.class);
        
        conf.setOutputKeyClass(PairOfInts.class);
        conf.setOutputValueClass(PairOfInts.class);

        // Delete the output directory if it exists already.
        Path outputDir = new Path(outputPath);
        FileSystem.get(conf).delete(outputDir, true);

        JobClient.runJob(conf);
        
        return 0;
    }

    public DedupSentencePairs() {}

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new DedupSentencePairs(), args);
    }
}
