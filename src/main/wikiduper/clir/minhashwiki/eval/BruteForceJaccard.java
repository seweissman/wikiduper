package wikiduper.clir.minhashwiki.eval;

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
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import wikiduper.clir.minhash.JaccardSim;
import wikiduper.utils.DocSentence;
import wikiduper.utils.Signature;
import edu.umd.cloud9.io.map.HMapSIW;
import edu.umd.cloud9.io.pair.PairOfFloatInt;
import edu.umd.cloud9.io.pair.PairOfIntFloat;
import edu.umd.cloud9.io.pair.PairOfLongInt;
import edu.umd.cloud9.io.pair.PairOfStrings;
import edu.umd.hooka.Vocab;
import edu.umd.hooka.alignment.HadoopAlign;
import edu.umd.hooka.ttables.TTable_monolithic_IFAs;

public class BruteForceJaccard extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(BruteForceJaccard.class);



    private static class SignatureMapper extends MapReduceBase implements
    Mapper<PairOfLongInt, PairOfStrings, PairOfIntFloat, DocSentence> {
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
        
        ArrayList<String> clusterReps = new ArrayList<String>();
        
        static HashSet<String> wordset = new HashSet<String>();
        public void map(PairOfLongInt key, PairOfStrings p, OutputCollector<PairOfIntFloat, DocSentence> output,
                    Reporter reporter) throws IOException {
            
            String lang = p.getLeftElement();
            String line = p.getRightElement();
            //System.out.println("key : " + key);
            //System.out.println("val : " + p);
            String[] tokens;
            int tokenct = 0;
            HMapSIW tokencts = new HMapSIW();
            DocSentence idOut;
            tokencts.clear();
            //System.out.println("nSamples = " + nSamples);

            // the "english" case
            if(lang.equals(eLang)){
                
                tokens = eTokenizer.processContent(line);
                tokenct = 0;
                for (String token : tokens) {
                    if (!tokencts.containsKey(token)) { // if this is first time we saw token in this sentence
                        tokenct++;
                    }
                    tokencts.increment(token);
                }
                
                // If the sentence meets min shingle ct requirements, emit the signature and the sentence/doc ID
                idOut = new DocSentence(key.getLeftElement(),key.getRightElement(),eLang);
                //System.out.println("idout = " + idOut);

                for(int c=0;c<clusterReps.size();c++){
                    String fline = clusterReps.get(c);                
                    tokens = fTokenizer.processContent(fline);
                    float maxsim = -1;
                    for(int l=0;l<nSamples;l++){
                        tokenct = 0;
                        tokencts.clear();
                        wordset.clear();
                        for (String ftoken : tokens) {
                            if (!tokencts.containsKey(ftoken)) { // if this is first time we saw token in this sentence
                                int f = fVocabSrc.get(ftoken);
                                if(f != -1){
                                    List<PairOfFloatInt> eSProbs = f2eProbs.get(f).getTranslationsWithProbsAsList(0.0f);
                                    float pr = samples[s%MAXSamples];
                                    s++;
                                    String eWord = sampleTranslateDistribution(eSProbs, pr, eVocabTgt);
                                    //  System.out.println("fword = " + ftoken + ", eword = " + eWord);
                                    wordset.add(eWord);
                                    tokenct++;
                                }else{
                                    wordset.add(ftoken);
                                    tokenct++;
                                }
                            }
                            tokencts.increment(ftoken);
                       }

                        float jsim = JaccardSim.jaccardSim(wordset.toArray(new String[wordset.size()]), tokens);
                        if(jsim > maxsim){
                            maxsim = jsim;
                        }
                    }
                    PairOfIntFloat clusterScore = new PairOfIntFloat();
                    clusterScore.set(c, maxsim);
                 }
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

        static Pattern linepat = Pattern.compile("(\\d+)\\t(\\d+)\\t([a-z][a-z])\\t(.*)$");
        public void configure(JobConf job) {
            rseed = job.getLong("rseed", 112345);
            Random r = new Random(rseed);            
            eLang = job.get("eLang");
            fLang = job.get("fLang");
            
            try {
                FileSystem fs = FileSystem.get(job);
                String clusterdir = job.get("clusters.in");
                FileStatus[] infiles;
                infiles = fs.globStatus(new Path(clusterdir + "/part-*"));
            int ct = 0;
            LongWritable cluster;
            LongWritable lastCluster;
            Text sentencevec;
            ArrayList<String> clusterList = new ArrayList<String>();
            //TITLESENTENCE.set(docid + "\t" + sentenceid + "\t" + langsentence.getLeftElement() + "\t" + langsentence.getRightElement());
            Random rcluster = new Random();
            for(FileStatus filestatus : infiles){
                System.out.println(filestatus.getPath().toString());
                try{
                FSDataInputStream in = fs.open(filestatus.getPath());
                SequenceFile.Reader reader;
                reader = new SequenceFile.Reader(job, SequenceFile.Reader.stream(in));
                cluster = new LongWritable();
                sentencevec=new Text();
                lastCluster = null;
                long linect = 0;
                long newnodect = 0;
                while(reader.next(cluster, sentencevec)){
                    if(lastCluster == null){
                        lastCluster = cluster;
                    }

                    if(!lastCluster.equals(cluster)){
                        int i = rcluster.nextInt(clusterList.size());
                        clusterReps.add(clusterList.get(i));
                        clusterList.clear();
                    }
                    Matcher m = linepat.matcher(sentencevec.toString());


                    if(m.matches() && m.groupCount() == 4){
                        long docid = Long.parseLong(m.group(1));
                        long sentenceid = Long.parseLong(m.group(2));
                        String lang = m.group(3);
                        String sentence = m.group(4);
                        DocSentence ds = new DocSentence();
                        ds.setId(docid);
                        ds.setSentence(sentenceid);
                        if(lang.equals(fLang)){
                            clusterList.add(sentence);
                        }
                        System.out.println(m.group(1) + "\t" + m.group(2) + "\t" + m.group(3) + "\t" + m.group(4));
                    }else{
                        System.out.println("BAD LINE: " + sentencevec);
                    }
                    
                    
                    
                }
                reader.close();
            }catch (EOFException e) {
                // For some reason it doesn't know when the input stream is done??
            }

            }
            
            } catch (IOException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }

            configureSampling(job,r);
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
    private static final String NUM_REDUCERS = "numReducers";
    
    private static final String CLUSTERSIN = "clustersin";    
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
        
        options.addOption(OptionBuilder.withArgName("dir").hasArg()
                .withDescription("cluster dir").create(CLUSTERSIN));

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
                || !cmdline.hasOption(CLUSTERSIN) || !cmdline.hasOption(nSamplesOption)
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

        String clusterDir = cmdline.getOptionValue(CLUSTERSIN);

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

        JobConf conf = new JobConf(getConf(), BruteForceJaccard.class);
        conf.setJobName(String.format("MinhashCLIR[%s: %s]", OUTPUT, outputPath));

        
        conf.set("clusters.in", clusterDir);
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
        
        conf.setNumMapTasks(20);
        conf.setNumReduceTasks(reduceTasks);

        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));

        conf.setMapperClass(SignatureMapper.class);
        
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

    public BruteForceJaccard() {}

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new BruteForceJaccard(), args);
    }
}
