package wikiduper.clir;

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


import ivory.core.tokenize.Tokenizer;
import ivory.core.tokenize.TokenizerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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

import wikiduper.hash.MultiplyShiftHash;
import wikiduper.wikipedia.WikipediaPage;
import wikiduper.wikipedia.WikipediaPageInputFormat;
import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.io.array.ArrayListOfLongsWritable;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.map.HMapSIW;
import edu.umd.cloud9.io.pair.PairOfFloatInt;
import edu.umd.cloud9.io.pair.PairOfIntString;
import edu.umd.cloud9.io.pair.PairOfStringInt;
import edu.umd.cloud9.io.pair.PairOfStrings;
import edu.umd.cloud9.util.array.ArrayListOfInts;
import edu.umd.cloud9.util.array.ArrayListOfLongs;
import edu.umd.hooka.Vocab;
import edu.umd.hooka.alignment.HadoopAlign;
import edu.umd.hooka.ttables.TTable_monolithic_IFAs;

public class CreateSentenceIdTranslationIdMap extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(CreateSentenceIdTranslationIdMap.class);

    /* SignatureeMapper
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
     */
    static int nSamples;
    
    private static class IdMapper extends MapReduceBase implements
    Mapper<IntWritable, ArrayListWritable<Text>, IntWritable, IntWritable> {
    //Mapper<LongWritable, WikipediaPage, ArrayListOfLongsWritable, PairOfStringInt> {
        
        public void map(IntWritable id, ArrayListWritable<Text> val, OutputCollector<IntWritable, IntWritable> output,
                    Reporter reporter) throws IOException {
            int idval = id.get();
            IntWritable idLine = new IntWritable();
            idLine.set(idval/nSamples);
            output.collect(idLine, id);
        }

        public void configure(JobConf job) {
            nSamples = job.getInt("nSamples", 100);
        }
        
    }

    /**
     * Emits groups of sentences that have the same hash signature. Only emit if there is more than one value for the key. 
     *
     */
    private static class IdReducer extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, ArrayListOfIntsWritable>{
        // collect all sentences that have hashed to the same hash signature
        @Override
        public void reduce(IntWritable key, Iterator<IntWritable> values,
                OutputCollector<IntWritable, ArrayListOfIntsWritable> output, Reporter reporter)
                        throws IOException {
            ArrayListOfIntsWritable outList = new ArrayListOfIntsWritable();
            while(values.hasNext()){
                outList.add(values.next().get());
            }

            output.collect(key, outList);

        }
    }
    
    private static final String OUTPUT = "output";
    private static final String INPUT = "input";
    private static final String nSamplesOption = "M";
    
    @SuppressWarnings("static-access")
    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("input").create(INPUT));
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

        if (!cmdline.hasOption(OUTPUT) || !cmdline.hasOption(INPUT) || !cmdline.hasOption(nSamplesOption)) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String nSamplesIn = cmdline.getOptionValue(nSamplesOption);
        String inputPath = cmdline.getOptionValue(INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);

        LOG.info("Tool name: " + this.getClass().getName());
        LOG.info(" - input file: " + inputPath);
        LOG.info(" - output file: " + outputPath);

        JobConf conf = new JobConf(getConf(), CreateSentenceIdTranslationIdMap.class);
        conf.setJobName(String.format("SampleSentencesCLIR[%s: %s]", OUTPUT, outputPath));

        
        conf.setInt("nSamples", Integer.parseInt(nSamplesIn));
        conf.setNumMapTasks(4);
        conf.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));

        conf.setMapperClass(IdMapper.class);
        conf.setReducerClass(IdReducer.class);
        
        //conf.setInputFormat(WikipediaPageInputFormat.class);
        conf.setInputFormat(SequenceFileInputFormat.class);
      //conf.setOutputFormat(SequenceFileOutputFormat.class);
        conf.setOutputFormat(MapFileOutputFormat.class);
        //conf.setOutputFormat(TextOutputFormat.class);
        
        // Set heap space - using old API
        conf.set("mapred.job.map.memory.mb", "2048");
        conf.set("mapred.map.child.java.opts", "-Xmx2048m");
        conf.set("mapred.job.reduce.memory.mb", "6144");
        conf.set("mapred.reduce.child.java.opts", "-Xmx6144m");
        //conf.set("mapred.child.java.opts", "-Xmx2048m");
        
        conf.setMapOutputKeyClass(IntWritable.class);
        conf.setMapOutputValueClass(IntWritable.class);
        
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(ArrayListOfIntsWritable.class);

        // Delete the output directory if it exists already.
        Path outputDir = new Path(outputPath);
        FileSystem.get(conf).delete(outputDir, true);
        
        JobClient.runJob(conf);

        return 0;
    }

    public CreateSentenceIdTranslationIdMap() {}

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new CreateSentenceIdTranslationIdMap(), args);
    }
}
