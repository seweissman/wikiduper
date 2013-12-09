package wikiduper.utils;

import ivory.core.tokenize.Tokenizer;
import ivory.core.tokenize.TokenizerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
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

import edu.umd.cloud9.io.array.ArrayListOfDoublesWritable;
import edu.umd.cloud9.io.map.HMapSIW;
import edu.umd.cloud9.io.pair.PairOfFloatInt;
import edu.umd.cloud9.io.pair.PairOfLongInt;
import edu.umd.cloud9.io.pair.PairOfStrings;
import edu.umd.hooka.Vocab;
import edu.umd.hooka.alignment.HadoopAlign;
import edu.umd.hooka.ttables.TTable_monolithic_IFAs;

public class BruteForcePwsim extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(BruteForcePwsim.class);

    private static class SignatureMapper extends MapReduceBase implements
    Mapper<PairOfLongInt, PairOfStrings, PairOfLongInt, ArrayListOfDoublesWritable> {
        
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
        static float samples[]; 

        // The minhash signature
        
        
        // Minhash variables
        static long seeds[];
        
        static int sigorder[]; 

        static int MINLEN;
        static int MAXLEN;
        //static int NSENTENCE = 3; // Number of sentences to match at a time
        // The minhash signature
        
        static HashMap<PairOfLongInt,ArrayList<HashSet<String>>> sampleTranslationSets = new HashMap<PairOfLongInt,ArrayList<HashSet<String>>>();
        
        static HashSet<String> wordset = new HashSet<String>();
        
        public void map(PairOfLongInt key, PairOfStrings p, OutputCollector<PairOfLongInt, ArrayListOfDoublesWritable> output,
                    Reporter reporter) throws IOException {
            
            String lang = p.getLeftElement();
            String line = p.getRightElement();
            //System.out.println("key : " + key);
            //System.out.println("val : " + p);
            String[] tokens;
            HMapSIW tokencts = new HMapSIW();
            tokencts.clear();

            // the "english" case
            if(lang.equals(eLang)){
                
                tokens = eTokenizer.processContent(line);
                if(tokens.length < MINLEN || tokens.length > MAXLEN) return;

                for (String token : tokens) {
                    tokencts.increment(token);
                }
                
                ArrayListOfDoublesWritable outScores = new ArrayListOfDoublesWritable();
                for(PairOfLongInt docIdSentenceCt : sampleTranslationSets.keySet()){
                    ArrayList<HashSet<String>> transList = sampleTranslationSets.get(docIdSentenceCt);
                    double maxsim = 0.0;
                    for(HashSet<String> trans : transList){
                        double sim = jaccardSim(tokencts.keySet(),trans);
                        if(sim > maxsim){
                            maxsim = sim;
                        }
                    }
                    outScores.add(maxsim);
                    
                }
                output.collect(key, outScores);
            }
        }
        
        public static double jaccardSim(Set<String> x, Set<String> y){
            int sharect = 0;
            for(String w : x){
                if(y.contains(w)){
                    sharect++;
                }
            }
            int total = x.size() + y.size() - sharect;
            return sharect*1.0/total;
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
            configureSampling(job,r);
            readSampleDocs(job);
        }

        public void readSampleDocs(JobConf job) {
            String docSampleFile = job.get("sampleDocs");
            HashSet<String> tokenSet;
            int scount = 0;
            FileSystem fs;
            try {
                fs = FileSystem.get(job);
            FileStatus[] infiles = fs.globStatus(new Path(docSampleFile + "/part-*"));
            for(FileStatus filestatus : infiles){
                try{
                    FSDataInputStream in = fs.open(filestatus.getPath());
                    SequenceFile.Reader reader;
                    reader = new SequenceFile.Reader(job, SequenceFile.Reader.stream(in));
                
                    PairOfStrings sentence = new PairOfStrings();
                    PairOfLongInt docIdSentenceCt = new PairOfLongInt();
                    while(reader.next(docIdSentenceCt, sentence)){
                        String s = sentence.getRightElement();
                        String[] tokens = fTokenizer.processContent(s);
                        ArrayList<HashSet<String>> sampleTranslationSet = new ArrayList<HashSet<String>>();
                        for(int l=0;l<nSamples;l++){
                            tokenSet = new HashSet<String>(); 
                            for (String ftoken : tokens) {
                                if (!tokenSet.contains(ftoken)) { // if this is first time we saw token in this sentence
                                    int f = fVocabSrc.get(ftoken);
                                    if(f != -1){
                                        List<PairOfFloatInt> eSProbs = f2eProbs.get(f).getTranslationsWithProbsAsList(0.0f);
                                        float pr = samples[scount%MAXSamples];
                                        scount++;    
                                        String eWord = sampleTranslateDistribution(eSProbs, pr, eVocabTgt);
                                        tokenSet.add(eWord);
                                    }else{
                                        tokenSet.add(ftoken);
                                    }
                                }
                            }
                            sampleTranslationSet.add(tokenSet);
                        }
                        sampleTranslationSets.put(docIdSentenceCt,sampleTranslationSet);
                        sentence = new PairOfStrings();
                        docIdSentenceCt = new PairOfLongInt();
                    }
                    reader.close();
                    }catch (EOFException e) {
                // For some reason it doesn't know when the input stream is done??
                    }catch (IOException e) {
                // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
            }
            } catch (IOException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }

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

    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    //private static final String NUM_REDUCERS = "numReducers";
    
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

    // Sample docs
    private static final String sampleDocsOption = "sampledocs";
    
    @SuppressWarnings("static-access")
    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("output path").create(OUTPUT));
        //options.addOption(OptionBuilder.withArgName("num").hasArg()
          //      .withDescription("number of reducers").create(NUM_REDUCERS));
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("input").create(INPUT));
        
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
        
        // Sample Docs
        
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("sampled documents").create(sampleDocsOption));
        
        
        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();
        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(OUTPUT) || !cmdline.hasOption(INPUT)
                || !cmdline.hasOption(nSamplesOption)
                || !cmdline.hasOption(eVocabSrcOption) || !cmdline.hasOption(fVocabSrcOption) 
                || !cmdline.hasOption(eVocabTgtOption) || !cmdline.hasOption(fVocabTgtOption)
                || !cmdline.hasOption(e2fProbsOption) || !cmdline.hasOption(f2eProbsOption)
                || !cmdline.hasOption(eLangOption) || !cmdline.hasOption(fLangOption) 
                || !cmdline.hasOption(eStopWordsOption) || !cmdline.hasOption(fStopWordsOption) 
                || !cmdline.hasOption(eTokensOption) || !cmdline.hasOption(fTokensOption)
                || !cmdline.hasOption(nSamplesOption) || !cmdline.hasOption(sampleDocsOption)
                
                ) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String inputPath = cmdline.getOptionValue(INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);
        //int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 4;

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

        String sampleDocsFile = cmdline.getOptionValue(sampleDocsOption);
        
        
        LOG.info("Tool name: " + this.getClass().getName());
        LOG.info(" - nput file: " + inputPath);
        LOG.info(" - output file: " + outputPath);

        JobConf conf = new JobConf(getConf(), BruteForcePwsim.class);
        conf.setJobName(String.format("BruteForcePwsim[%s: %s]", OUTPUT, outputPath));

        conf.setLong("rseed", 1123456);
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

        conf.set("sampleDocs", sampleDocsFile);
        
        conf.setNumMapTasks(20);
        conf.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));

        conf.setMapperClass(SignatureMapper.class);
        
        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        
        // Set heap space - using old API
        conf.set("mapred.job.map.memory.mb", "2048");
        conf.set("mapred.map.child.java.opts", "-Xmx2048m");
        conf.set("mapred.job.reduce.memory.mb", "8000");
        conf.set("mapred.reduce.child.java.opts", "-Xmx8000m");
        //conf.set("mapred.child.java.opts", "-Xmx2048m");
        
        conf.setMapOutputKeyClass(PairOfLongInt.class);
        conf.setMapOutputValueClass(ArrayListOfDoublesWritable.class);
        
        // Delete the output directory if it exists already.
        Path outputDir = new Path(outputPath);
        FileSystem.get(conf).delete(outputDir, true);
        
        JobClient.runJob(conf);

        return 0;
    }

    public BruteForcePwsim() {}

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new BruteForcePwsim(), args);
    }
}
