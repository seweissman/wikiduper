package wikiduper.clir.minhashwiki;

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
import java.util.Set;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import wikiduper.hash.MultiplyShiftHash;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.map.HMapSIW;
import edu.umd.cloud9.io.pair.PairOfFloatInt;
import edu.umd.cloud9.io.pair.PairOfLongInt;
import edu.umd.cloud9.io.pair.PairOfStrings;
import edu.umd.hooka.Vocab;
import edu.umd.hooka.alignment.HadoopAlign;
import edu.umd.hooka.ttables.TTable_monolithic_IFAs;

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
    Mapper<PairOfLongInt, PairOfStrings, Signature, DocSentence> {
    //Mapper<LongWritable, WikipediaPage, ArrayListOfLongsWritable, PairOfStringInt> {
        // Sampling variables
        
        static long rseed;

        static String eLang;
        static String fLang;
        static String eTokensFile;
        static String fTokensFile;
        static String eStopWordsFile;
        static String fStopWordsFile;
        static int nSamples;
        static long sampleSeed;
        static Vocab fVocabSrc;
        static Vocab eVocabTgt;
        static TTable_monolithic_IFAs e2fProbs;
        static TTable_monolithic_IFAs f2eProbs;
        static Tokenizer eTokenizer;
        static Tokenizer fTokenizer;
        //static Random rSample;
        static int MAXSamples = 100000;
        static int s = 0;
        static float samples[]; 
        int m;
        
        // The minhash signature
        
        
        // Minhash variables
        static long seeds[];
        
        static int sigorder[]; 
        static long minhash[];

        static int nHash; // Total number of hashes per sentence
        static int K; // Length of hash vector
        static int N; // Number of hashes per input sentence (N < NHASH)
        static int NHASHOUTPUTBITS;
        static int MINLEN;
        static int MAXLEN;
        //static int NSENTENCE = 3; // Number of sentences to match at a time
        static MultiplyShiftHash hashfamily;
        static Signature outsig = new Signature(K);
        // The minhash signature
        
        static HashSet<String> wordset = new HashSet<String>();
        static HashSet<String> sigMap = new HashSet<String>();
        public void map(PairOfLongInt key, PairOfStrings p, OutputCollector<Signature, DocSentence> output,
                    Reporter reporter) throws IOException {
            
            String outstr;
            String lang = p.getLeftElement();
            String line = p.getRightElement();
            //System.out.println("key : " + key);
            //System.out.println("val : " + p);
            String[] tokens;
            int tokenct = 0;
            HMapSIW tokencts = new HMapSIW();
            DocSentence idOut;
            Text outWord;
            tokencts.clear();
            //System.out.println("nSamples = " + nSamples);

            // the "english" case
            if(lang.equals(eLang)){
                
                tokens = eTokenizer.processContent(line);
                if(tokens.length < MINLEN || tokens.length > MAXLEN) return;
                //System.out.print("eline " + line + "\n");
                //System.out.print("etokens ");
                //for(String t : tokens){
                  //  System.out.print(t + " ");
                //}
                //System.out.println();
                tokenct = 0;
                //outstr = "";
                for (String token : tokens) {
                    if (!tokencts.containsKey(token)) { // if this is first time we saw token in this sentence
                       //if(tokenct != 0) outstr += ",";
                       //outstr += token;
                        tokenct++;
                    }
                    tokencts.increment(token);
                }
                
                // If the sentence meets min shingle ct requirements, emit the signature and the sentence/doc ID
                idOut = new DocSentence(key.getLeftElement(),key.getRightElement(),eLang);
                //System.out.println("idout = " + idOut);
                doMinhash(idOut,tokencts.keySet(),output);
            
            }else if(lang.equals(fLang)){
                
                tokens = fTokenizer.processContent(line);
                if(tokens.length < MINLEN || tokens.length > MAXLEN) return;

                sigMap.clear();
                
                for(int l=0;l<nSamples;l++){
                    tokenct = 0;
                    tokencts.clear();
                    wordset.clear();
                    outstr = "";
                    for (String ftoken : tokens) {
                        if (!tokencts.containsKey(ftoken)) { // if this is first time we saw token in this sentence
                            int f = fVocabSrc.get(ftoken);
                            if(f != -1){
                                List<PairOfFloatInt> eSProbs = f2eProbs.get(f).getTranslationsWithProbsAsList(0.0f);
                                float pr = samples[s%MAXSamples];
                                s++;
                                String eWord = sampleTranslateDistribution(eSProbs, pr, eVocabTgt);
                                //System.out.println("fword = " + ftoken + ", eword = " + eWord);
                                wordset.add(eWord);
                                if(tokenct != 0) outstr += ",";
                                outstr += eWord;
                                tokenct++;
                            }else{
                                wordset.add(ftoken);
                                if(tokenct != 0) outstr += ",";
                                outstr += ftoken;
                                tokenct++;
                            }
                        }
                        tokencts.increment(ftoken);
                    }
                    
                    
                    if(!sigMap.contains(outstr) && tokenct >= MINLEN && tokenct <= MAXLEN){
                    //if(tokenct >= MINLEN && tokenct <= MAXLEN){
                        idOut = new DocSentence(key.getLeftElement(),key.getRightElement(),fLang);
                        doMinhash(idOut,wordset,output);
                    }
                    sigMap.add(outstr);
                    

                }
            }


        }
        
        public static void doMinhash(DocSentence idOut, Set<String> set, OutputCollector<Signature, DocSentence> output) throws IOException{
            //int tokenct = 0;
            //HMapSIW sent = new HMapSIW();
                        
            String hashval[] = new String[nHash];
            // Process F sentence;
            for(int i=0;i<nHash;i++){
                minhash[i] = Long.MAX_VALUE;
            }
            //sent.clear();
            for (String token : set) {
                String tokenstr = token;
              //  if (!sent.containsKey(tokenstr)) { // if this is first time we saw token in this sentence
                    //tokenct++;
                    long hash[] = hashfamily.hash(tokenstr);
                    for(int j=0;j<nHash;j++){
                        if(hash[j] < minhash[j]){
                            minhash[j] = hash[j];
                            hashval[j] = tokenstr;
                        }
                    }

                //    sent.increment(tokenstr);
                //}
            }
            // If the sentence meets min shingle ct requirements, emit the signature and the sentence/doc ID
            //if(tokenct > MINLEN && tokenct < MAXLEN){
                // generate N k-minhash-signatures
                //  start from same seed, otherwise doesn't work so well
            int r=0;
                for(int j=0; j<N; j++){
                    outsig = new Signature(K);
                    for(int i=0; i<K; i++){
                        int x = sigorder[r]; //r.nextInt(nHash);
                        r++;
                        outsig.set(i, minhash[x]);
                    }
                    //System.out.println("fsig " + outsig);
                    output.collect(outsig, idOut);
                }
        }
        
        public static String sampleTranslateDistribution(List<PairOfFloatInt> eSProbs, float p, Vocab eVocab){
            Iterator<PairOfFloatInt> it = eSProbs.iterator();
            PairOfFloatInt probe = null;
            int e = -1;
            float psum = 0;
            while(psum <= p && it.hasNext()){
                probe = it.next();
                psum += probe.getLeftElement();
                e = probe.getRightElement();
            }
            String eWord = eVocab.get(e);
            return eWord;
        }

        
        public void configure(JobConf job) {
            rseed = job.getLong("rseed", 112345);
            Random r = new Random(rseed);            
            configureMinhash(job, r);
            configureSampling(job,r);
        }

        public void configureMinhash(JobConf job, Random r){
            nHash = job.getInt("NHASH", 20);
            NHASHOUTPUTBITS = job.getInt("NHASHOUTPUTBITS", 30);
            MINLEN = job.getInt("MINLEN", 10);
            MAXLEN = job.getInt("MAXLEN", 100);
            K = job.getInt("K",  10);
            N = job.getInt("N", 10);
            seeds = new long[nHash];
            int ct = 0;
            while(ct < nHash){
                seeds[ct] = r.nextLong();
                ct++;
            }
            
            long sigseed = r.nextLong();
            Random rsig = new Random(sigseed);
            sigorder = new int[N*K];
            for(int i=0;i<N*K;i++){
                sigorder[i] = rsig.nextInt(nHash);
            }
            hashfamily = new MultiplyShiftHash(NHASHOUTPUTBITS,seeds);
            minhash = new long[nHash];
            System.out.println("N = " + N);
            System.out.println("K = " + K);
            System.out.println("nHash = " + nHash);
            System.out.println("nSamples = " + nSamples);
        }

        public void configureSampling(JobConf job, Random r){

            nSamples = job.getInt("nSamples", 100);
            eTokensFile = job.get("eTokensFile");
            fTokensFile = job.get("fTokensFile");
            eStopWordsFile = job.get("eStopWordsFile");
            fStopWordsFile = job.get("fStopWordsFile");
            System.out.println("eStopWordsFile" + eStopWordsFile);
            System.out.println("fStopWordsFile" + fStopWordsFile);

            sampleSeed = r.nextLong();
            Random rSample = new Random(sampleSeed);
            samples = new float[MAXSamples];
            for(int i=0;i<MAXSamples;i++){
                samples[i] = rSample.nextFloat();
            }
            
            eLang = job.get("eLang");
            fLang = job.get("fLang");
            
            String eVocabSrcFile = job.get("eVocabSrcFile");
            String eVocabTgtFile = job.get("eVocabTgtFile");
            String fVocabSrcFile = job.get("fVocabSrcFile");
            String fVocabTgtFile = job.get("fVocabTgtFile");
            String probTablef2eFile = job.get("probTablef2eFile");
            String probTablee2fFile = job.get("probTablee2fFile");
            
            eTokenizer = TokenizerFactory.createTokenizer(eLang, eTokensFile, true, eStopWordsFile, eStopWordsFile + ".stemmed", null);
            fTokenizer = TokenizerFactory.createTokenizer(fLang, fTokensFile, true, fStopWordsFile, fStopWordsFile + ".stemmed", null);
            
            FileSystem fs;
            try {
                fs = FileSystem.get(job);
                fVocabSrc = HadoopAlign.loadVocab(new Path(fVocabSrcFile), fs);
                //eVocabTgt = HadoopAlign.loadVocab(new Path(eVocabTgtFile), fs);
                //fVocabSrc = HadoopAlign.loadVocab(new Path(fVocabSrcFile), fs);
                eVocabTgt = HadoopAlign.loadVocab(new Path(eVocabTgtFile), fs);
        
                try{
                    f2eProbs = new TTable_monolithic_IFAs(fs, new Path(probTablef2eFile), true);
                }catch(EOFException e){}
                try{
                    e2fProbs = new TTable_monolithic_IFAs(fs, new Path(probTablee2fFile), true);
                }catch(EOFException e){}

            } catch (IOException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
        }
        
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
    private static class SignatureReducer extends MapReduceBase implements Reducer<Signature, DocSentence, Signature, DocSentence> {

        // collect all sentences that have hashed to the same hash signature
        static ArrayListWritable<DocSentence> nearDuplicateSentenceList = new ArrayListWritable<DocSentence>();
        static final HashSet<String> valset = new HashSet<String>();
        @Override
        public void reduce(Signature key, Iterator<DocSentence> values,
                OutputCollector<Signature, DocSentence> output, Reporter reporter)
                        throws IOException {
            DocSentence valout;
            nearDuplicateSentenceList = new ArrayListWritable<DocSentence>();
            valset.clear();
            //System.out.println("key: " + key);
            //System.out.print("values: ");
            while (values.hasNext()) {
                DocSentence id = values.next();
                String valstr = id.toString();
                if(!valset.contains(valstr)){
                    valout = new DocSentence(id.getId(),id.getSentence(),id.getLanguage());
                    nearDuplicateSentenceList.add(valout);
                }
                valset.add(valstr);
            }
            //System.out.println();
            //System.out.println(nearDuplicateSentenceList.size());
            //System.out.println("key " + key);
            //System.out.println("output " + nearDuplicateSentenceList);
            if(nearDuplicateSentenceList.size() == 1) return;
            //System.out.println("nearDuplicateSentenceList " + nearDuplicateSentenceList);
            for(DocSentence ds : nearDuplicateSentenceList){
                output.collect(key, ds);
            }

        }
    }
    
    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String NUM_REDUCERS = "numReducers";
    
    //Minhash options
    private static final String NHASH_IN = "nHash";
    private static final String K_IN = "k";
    private static final String N_IN = "n";
    private static final String HASHBITS = "bits";
    
    //Sampling Options
    private static final String eVocabSrcOption = "eVocabSrc";
    private static final String fVocabSrcOption = "fVocabSrc";
    private static final String eStopWordsOption = "eStopWords";
    private static final String fStopWordsOption = "fStopWords";
    private static final String eTokensOption = "eTokens";
    private static final String fTokensOption = "fTokens";
    private static final String eLangOption = "eLang";
    private static final String fLangOption = "fLang";
    private static final String eVocabTgtOption = "eVocabTgt";
    private static final String fVocabTgtOption = "fVocabTgt";
    private static final String e2fProbsOption = "e2fprobs";
    private static final String f2eProbsOption = "f2eprobs";
    private static final String nSamplesOption = "M";

    
    
    @SuppressWarnings("static-access")
    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("number of reducers").create(NUM_REDUCERS));
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("input").create(INPUT));
        
        // Minhsh Options
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("number of hashes").create(NHASH_IN));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("length of minhash signature vector").create(K_IN));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("number of signatures").create(N_IN));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("size of hash in bits").create(HASHBITS));

        // Sampling Options
        options.addOption(OptionBuilder.withArgName("string")
                .hasArg().withDescription("e language").create(eLangOption));
        options.addOption(OptionBuilder.withArgName("string")
                .hasArg().withDescription("f language").create(fLangOption));
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("e stop words").create(eStopWordsOption));
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("f stop words").create(fStopWordsOption));
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("e tokens").create(eTokensOption));
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("f tokens").create(fTokensOption));
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("e vocab src").create(eVocabSrcOption));
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("f vocab src").create(fVocabSrcOption));
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("e vocab tgt").create(eVocabTgtOption));
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("f vocab tgt").create(fVocabTgtOption));
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("e2f prob table").create(e2fProbsOption));
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("f2e prob table").create(f2eProbsOption));
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("integer")
                .hasArg().withDescription("n samples").create(nSamplesOption));
        
        
        
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
                || !cmdline.hasOption(HASHBITS) || !cmdline.hasOption(nSamplesOption)
                || !cmdline.hasOption(eVocabSrcOption) || !cmdline.hasOption(fVocabSrcOption) 
                || !cmdline.hasOption(eVocabTgtOption) || !cmdline.hasOption(fVocabTgtOption)
                || !cmdline.hasOption(e2fProbsOption) || !cmdline.hasOption(f2eProbsOption)
                || !cmdline.hasOption(eLangOption) || !cmdline.hasOption(fLangOption) 
                || !cmdline.hasOption(eStopWordsOption) || !cmdline.hasOption(fStopWordsOption) 
                || !cmdline.hasOption(eTokensOption) || !cmdline.hasOption(fTokensOption)
                || !cmdline.hasOption(nSamplesOption)
                
                ) {
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

        String eLang = cmdline.getOptionValue(eLangOption);
        String fLang = cmdline.getOptionValue(fLangOption);
        String eTokensPath = cmdline.getOptionValue(eTokensOption);
        String fTokensPath = cmdline.getOptionValue(fTokensOption);
        String eStopWordsPath = cmdline.getOptionValue(eStopWordsOption);
        String fStopWordsPath = cmdline.getOptionValue(fStopWordsOption);
        String eVocabSrcPath = cmdline.getOptionValue(eVocabSrcOption);
        String fVocabSrcPath = cmdline.getOptionValue(fVocabSrcOption);
        String eVocabTgtPath = cmdline.getOptionValue(eVocabTgtOption);
        String fVocabTgtPath = cmdline.getOptionValue(fVocabTgtOption);
        String e2fProbsPath = cmdline.getOptionValue(e2fProbsOption);
        String f2eProbsPath = cmdline.getOptionValue(f2eProbsOption);
        int nSamples = Integer.parseInt(cmdline.getOptionValue(nSamplesOption));

        
        
        LOG.info("Tool name: " + this.getClass().getName());
        LOG.info(" - nput file: " + inputPath);
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
        conf.setInt("nSamples", nSamples);

        conf.set("eLang", eLang);
        conf.set("fLang", fLang);
        conf.set("eTokensFile", eTokensPath);
        conf.set("fTokensFile", fTokensPath);
        conf.set("eStopWordsFile", eStopWordsPath);
        conf.set("fStopWordsFile", fStopWordsPath);
        conf.set("eVocabSrcFile", eVocabSrcPath);
        conf.set("eVocabTgtFile", eVocabTgtPath);
        conf.set("fVocabSrcFile", fVocabSrcPath);
        conf.set("fVocabTgtFile", fVocabTgtPath);
        conf.set("probTablef2eFile", f2eProbsPath);
        conf.set("probTablee2fFile", e2fProbsPath);


        
        conf.setNumMapTasks(20);
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
        conf.set("mapred.job.reduce.memory.mb", "8000");
        conf.set("mapred.reduce.child.java.opts", "-Xmx8000m");
        //conf.set("mapred.child.java.opts", "-Xmx2048m");
        
        conf.setMapOutputKeyClass(Signature.class);
        conf.setMapOutputValueClass(DocSentence.class);
        
        conf.setOutputKeyClass(Signature.class);
        conf.setOutputValueClass(DocSentence.class);

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
