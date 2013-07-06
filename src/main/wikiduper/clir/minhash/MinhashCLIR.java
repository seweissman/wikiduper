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


import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
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
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import wikiduper.hash.MultiplyShiftHash;
import edu.umd.cloud9.io.array.ArrayListOfLongsWritable;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.map.HMapSIW;
import edu.umd.cloud9.io.pair.PairOfFloatInt;
import edu.umd.hooka.Vocab;

public class MinhashCLIR extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(MinhashCLIR.class);

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

    private static class SignatureMapper extends MapReduceBase implements
    Mapper<IntWritable, ArrayListWritable<Text>, ArrayListOfLongsWritable, IntWritable> {
    //Mapper<LongWritable, WikipediaPage, ArrayListOfLongsWritable, PairOfStringInt> {
        
        static long rseed;
        static long seeds[];
        static long sigseed; // Seed to use when randoly selecting signature vectors
        static long minhash[];

        static int nHash; // Total number of hashes per sentence
        static int K; // Length of hash vector
        static int N; // Number of hashes per input sentence (N < NHASH)
        static int NHASHOUTPUTBITS;
        static int MINLEN;
        static int MAXLEN;
        //static int NSENTENCE = 3; // Number of sentences to match at a time
        static MultiplyShiftHash hashfamily;
        static int nSamples;
        static ArrayListOfLongsWritable outsig = new ArrayListOfLongsWritable(K);
        // The minhash signature

        public void map(IntWritable key, ArrayListWritable<Text> tokens, OutputCollector<ArrayListOfLongsWritable, IntWritable> output,
                    Reporter reporter) throws IOException {
            int tokenct = 0;
            HMapSIW sent = new HMapSIW();
            Random r;
            String hashval[] = new String[nHash];
            // Process F sentence;
            for(int i=0;i<nHash;i++){
                minhash[i] = Long.MAX_VALUE;
            }
            sent.clear();
            for (Text token : tokens) {
                String tokenstr = token.toString();
                if (!sent.containsKey(tokenstr)) { // if this is first time we saw token in this sentence
                    tokenct++;
                    long hash[] = hashfamily.hash(tokenstr);
                    for(int j=0;j<nHash;j++){
                        if(hash[j] < minhash[j]){
                            minhash[j] = hash[j];
                            hashval[j] = tokenstr;
                        }
                    }

                    sent.increment(tokenstr);
                }
            }
            // If the sentence meets min shingle ct requirements, emit the signature and the sentence/doc ID
            //if(tokenct > MINLEN && tokenct < MAXLEN){
                // generate N k-minhash-signatures
                //  start from same seed, otherwise doesn't work so well
                    
                r = new Random(sigseed);
                for(int j=0; j<N; j++){
                    outsig = new ArrayListOfLongsWritable();
                    for(int i=0; i<K; i++){
                        outsig.add(i, 0);
                    }
                    for(int i=0; i<K; i++){
                        int x = r.nextInt(nHash);
                        outsig.set(i, minhash[x]);
                    }
                    //System.out.println("fsig " + outsig);
                    output.collect(outsig, key);
                }
            //}

        }

        
        public void configure(JobConf job) {
            rseed = job.getLong("rseed", 112345);
            nHash = job.getInt("NHASH", 20);
            NHASHOUTPUTBITS = job.getInt("NHASHOUTPUTBITS", 30);
            MINLEN = job.getInt("MINLEN", 5);
            MAXLEN = job.getInt("MAXLEN", 100);
            K = job.getInt("K",  10);
            N = job.getInt("N", 10);
            nSamples = job.getInt("nSamples", 100);
            seeds = new long[nHash];
            Random r = new Random(rseed);
            int ct = 0;
            while(ct < nHash){
                seeds[ct] = r.nextLong();
                ct++;
            }
            sigseed = r.nextLong();
            hashfamily = new MultiplyShiftHash(NHASHOUTPUTBITS,seeds);
            minhash = new long[nHash];
            System.out.println("N = " + N);
            System.out.println("K = " + K);
            System.out.println("nHash = " + nHash);
            System.out.println("nSamples = " + nSamples);

        }
    }

    public static String sampleTranslateDistribution(List<PairOfFloatInt> fSProbs, float p, Vocab fVocab){
            Iterator<PairOfFloatInt> it = fSProbs.iterator();
            PairOfFloatInt probf = null;
            int f = -1;
            float psum = 0;
            while(psum <= p && it.hasNext()){
                probf = it.next();
                psum += probf.getLeftElement();
                f = probf.getRightElement();
            }
            String fWord = fVocab.get(f);
            return fWord;
        }

    /**
     * Emits groups of sentences that have the same hash signature. Only emit if there is more than one value for the key. 
     *
     */
    /*
    private static class SignatureReducer extends MapReduceBase implements Reducer<ArrayListOfLongsWritable, PairOfStringInt, PairOfStringInt, PairOfStringInt> {

        // collect all sentences that have hashed to the same hash signature
        //static final ArrayListWritable<PairOfStringInt> nearDuplicateSentenceList = new ArrayListWritable<PairOfStringInt>();
        ArrayList<PairOfStringInt> sentenceList = new ArrayList<PairOfStringInt>();
        @Override
        public void reduce(ArrayListOfLongsWritable key, Iterator<PairOfStringInt> values,
                OutputCollector<PairOfStringInt, PairOfStringInt> output, Reporter reporter)
                        throws IOException {
            sentenceList.clear();

            while (values.hasNext()) {
                PairOfStringInt val = values.next().clone();
                sentenceList.add(val);
            }
            
            if(sentenceList.size() == 1) return;

            for(int i=0;i<sentenceList.size();i++){
                for(int j=i+1;j<sentenceList.size();j++){
                    output.collect(sentenceList.get(i), sentenceList.get(j));
            
                }
            }
        }
    }
    */
    /**
     * Emits groups of sentences that have the same hash signature. Only emit if there is more than one value for the key. 
     *
     */
    private static class SignatureReducer extends MapReduceBase implements Reducer<ArrayListOfLongsWritable, IntWritable, ArrayListOfLongsWritable, ArrayListWritable<IntWritable>> {

        // collect all sentences that have hashed to the same hash signature
        static ArrayListWritable<IntWritable> nearDuplicateSentenceList = new ArrayListWritable<IntWritable>();
        static final HashSet<Integer> valset = new HashSet<Integer>();
        @Override
        public void reduce(ArrayListOfLongsWritable key, Iterator<IntWritable> values,
                OutputCollector<ArrayListOfLongsWritable, ArrayListWritable<IntWritable>> output, Reporter reporter)
                        throws IOException {
            IntWritable valout;
            nearDuplicateSentenceList = new ArrayListWritable<IntWritable>();
            valset.clear();
            //System.out.print("values: ");
            while (values.hasNext()) {
                IntWritable val = values.next();
                //System.out.print(val + " ");
                if(!valset.contains(val.get())){
                    valout = new IntWritable();
                    valout.set(val.get());
                    nearDuplicateSentenceList.add(valout);
                }
                valset.add(val.get());
            }
            //System.out.println();
            //System.out.println("output " + nearDuplicateSentenceList);
            if(nearDuplicateSentenceList.size() == 1) return;
            output.collect(key, nearDuplicateSentenceList);

        }
    }
    
    private static final String OUTPUT = "output";
    private static final String INPUT = "input";
    private static final String NUM_REDUCERS = "numReducers";
    private static final String NHASH_IN = "nHash";
    private static final String K_IN = "k";
    private static final String N_IN = "n";
    private static final String HASHBITS = "bits";
    private static final String nSamplesOption = "M";
    
    static int NHASH; // Total number of hashes per sentence
    static int K; // Length of hash vector
    static int N; // Number of hashes per input sentence (N < NHASH)
    static int NHASHOUTPUTBITS;
    
    
    @SuppressWarnings("static-access")
    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("input").create(INPUT));
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("number of reducers").create(NUM_REDUCERS));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("number of hashes").create(NHASH_IN));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("length of minhash signature vector").create(K_IN));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("number of signatures").create(N_IN));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("size of hash in bits").create(HASHBITS));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("n Samples").create(nSamplesOption));
        
        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();
        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(OUTPUT) || !cmdline.hasOption(INPUT) 
                || !cmdline.hasOption(NHASH_IN) || !cmdline.hasOption(K_IN) || !cmdline.hasOption(N_IN)
                || !cmdline.hasOption(HASHBITS) || !cmdline.hasOption(nSamplesOption)) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String inputPath = cmdline.getOptionValue(INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);
        int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 4;
        int nHash = Integer.parseInt(cmdline.getOptionValue(NHASH_IN));
        int k = Integer.parseInt(cmdline.getOptionValue(K_IN));
        int n = Integer.parseInt(cmdline.getOptionValue(N_IN));
        int nBits = Integer.parseInt(cmdline.getOptionValue(HASHBITS)); 
        int nSamples = Integer.parseInt(cmdline.getOptionValue(nSamplesOption));

        LOG.info("Tool name: " + this.getClass().getName());
        LOG.info(" - input file: " + inputPath);
        LOG.info(" - output file: " + outputPath);
        LOG.info(" - number hashes: " + nHash);
        LOG.info(" - hash bits: " + nBits);
        LOG.info(" - hash sig length: " + k);
        LOG.info(" - num hash sigs: " + n);

        JobConf conf = new JobConf(getConf(), MinhashCLIR.class);
        conf.setJobName(String.format("MinhashCLIR[%s: %s]", OUTPUT, outputPath));

        conf.setLong("rseed", 1123456);
        //conf.setInt("NHASH", 20);
        conf.setInt("NHASH", nHash);
        //conf.setInt("NHASHOUTPUTBITS", 30);
        conf.setInt("NHASHOUTPUTBITS", nBits);
        //conf.setInt("K",  10);
        conf.setInt("K",  k);
        //conf.setInt("N", 10);
        conf.setInt("N", n);
        conf.setInt("M", 100);
        conf.setInt("MINLEN", 5);
        conf.setInt("MAXLEN", 100);
        conf.setInt("nSamples", nSamples);


        conf.setNumMapTasks(4);
        conf.setNumReduceTasks(reduceTasks);

        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));

        conf.setMapperClass(SignatureMapper.class);
        conf.setReducerClass(SignatureReducer.class);
        
        //conf.setInputFormat(WikipediaPageInputFormat.class);
        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        //conf.setOutputFormat(TextOutputFormat.class);
        
        // Set heap space - using old API
        conf.set("mapred.job.map.memory.mb", "2048");
        conf.set("mapred.map.child.java.opts", "-Xmx2048m");
        conf.set("mapred.job.reduce.memory.mb", "6144");
        conf.set("mapred.reduce.child.java.opts", "-Xmx6144m");
        //conf.set("mapred.child.java.opts", "-Xmx2048m");
        
        conf.setMapOutputKeyClass(ArrayListOfLongsWritable.class);
        conf.setMapOutputValueClass(IntWritable.class);
        
        conf.setOutputKeyClass(ArrayListOfLongsWritable.class);
        conf.setOutputValueClass(ArrayListWritable.class);

        // Delete the output directory if it exists already.
        Path outputDir = new Path(outputPath);
        FileSystem.get(conf).delete(outputDir, true);
        
        JobClient.runJob(conf);

        return 0;
    }

    public MinhashCLIR() {}

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new MinhashCLIR(), args);
    }
}
