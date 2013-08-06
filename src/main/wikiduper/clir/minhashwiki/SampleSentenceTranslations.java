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
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.io.array.ArrayListOfLongsWritable;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.map.HMapSIW;
import edu.umd.cloud9.io.pair.PairOfFloatInt;
import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.io.pair.PairOfLongInt;
import edu.umd.cloud9.io.pair.PairOfStrings;
import edu.umd.hooka.Vocab;
import edu.umd.hooka.alignment.HadoopAlign;
import edu.umd.hooka.ttables.TTable_monolithic_IFAs;

public class SampleSentenceTranslations extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(SampleSentenceTranslations.class);

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

    private static class SampleMapper extends MapReduceBase implements
    Mapper<PairOfLongInt, PairOfStrings, ArrayListOfLongsWritable, ArrayListWritable<Text>> {
        
        static long rseed;

        //static int MINLEN;
        //static int MAXLEN;

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
        static ArrayListWritable<Text> outsig;
        static Random rSample;
        // The minhash signature

        public void map(PairOfLongInt key, PairOfStrings p, OutputCollector<ArrayListOfLongsWritable, ArrayListWritable<Text>> output,
                    Reporter reporter) throws IOException {
            String outstr;
            String lang = p.getLeftElement();
            String line = p.getRightElement();
            //System.out.println("key : " + key);
            //System.out.println("val : " + p);
            String[] tokens;
            int tokenct = 0;
            HMapSIW sent = new HMapSIW();
            ArrayListOfLongsWritable idOut;
            Text outWord;
            sent.clear();
            //System.out.println("nSamples = " + nSamples);

            // the "english" case
            if(lang.equals(eLang)){
                
                tokens = eTokenizer.processContent(line);
                //System.out.print("eline " + line + "\n");
                //System.out.print("etokens ");
                //for(String t : tokens){
                  //  System.out.print(t + " ");
                //}
                //System.out.println();
                tokenct = 0;
                outstr = "";
                outsig = new ArrayListWritable<Text>();
                for (String token : tokens) {
                    if (!sent.containsKey(token)) { // if this is first time we saw token in this sentence
                        outWord = new Text();
                        outWord.set(token);
                        if(tokenct != 0) outstr += ",";
                        outstr += token;
                        outsig.add(outWord);
                        tokenct++;
                    }
                    sent.increment(token);
                }
                
                // If the sentence meets min shingle ct requirements, emit the signature and the sentence/doc ID
                idOut = new ArrayListOfLongsWritable();
                idOut.add(key.getLeftElement());
                idOut.add(key.getRightElement());
                idOut.add(-1);
                //System.out.println("idout = " + idOut);
                output.collect(idOut, outsig);
            
            }else if(lang.equals(fLang)){
                
                tokens = fTokenizer.processContent(line);
                //System.out.print("fline " + line + "\n");
                //System.out.print("ftokens ");
                //for(String t : tokens){
                  //  System.out.print(t + " ");
                //}
                //System.out.println();

                HashSet<String> sigMap = new HashSet<String>();
                for(int l=0;l<nSamples;l++){
                    tokenct = 0;
                    sent.clear();
                    outstr = "";
                    outsig = new ArrayListWritable<Text>();
                    for (String ftoken : tokens) {
                        if (!sent.containsKey(ftoken)) { // if this is first time we saw token in this sentence
                            int f = fVocabSrc.get(ftoken);
                            if(f != -1){
                                List<PairOfFloatInt> eSProbs = f2eProbs.get(f).getTranslationsWithProbsAsList(0.0f);
                                float pr = rSample.nextFloat();
                                String eWord = sampleTranslateDistribution(eSProbs, pr, eVocabTgt);
                                //System.out.println("fword = " + ftoken + ", eword = " + eWord);
                                outWord = new Text();
                                outWord.set(eWord);
                                outsig.add(outWord);
                                if(tokenct != 0) outstr += ",";
                                outstr += eWord;
                                tokenct++;
                            }
                        }
                        sent.increment(ftoken);
                    }
                    
                    idOut = new ArrayListOfLongsWritable();
                    if(!sigMap.contains(outstr)){
                        idOut.add(key.getLeftElement());
                        idOut.add(key.getRightElement());
                        idOut.add(l);
                        output.collect(idOut, outsig);
                    }
                    sigMap.add(outstr);
                    

                }
            }
        }

        
        public void configure(JobConf job) {
            rseed = job.getLong("rseed", 112345);
            //MINLEN = job.getInt("MINLEN", 5);
            //MAXLEN = job.getInt("MAXLEN", 100);
            nSamples = job.getInt("nSamples", 100);
            eTokensFile = job.get("eTokensFile");
            fTokensFile = job.get("fTokensFile");
            eStopWordsFile = job.get("eStopWordsFile");
            fStopWordsFile = job.get("fStopWordsFile");
            System.out.println("eStopWordsFile" + eStopWordsFile);
            System.out.println("fStopWordsFile" + fStopWordsFile);

            Random r = new Random(rseed);
            sampleSeed = r.nextLong();
            rSample = new Random(sampleSeed);
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
    private static class SampleReducer extends MapReduceBase implements Reducer<Text, IntWritable, IntWritable, ArrayListWritable<Text>> {

        // collect all sentences that have hashed to the same hash signature
        static int ct = 0;
        @Override
        public void reduce(Text key, Iterator<IntWritable> values,
                OutputCollector<IntWritable, ArrayListWritable<Text>> output, Reporter reporter)
                        throws IOException {
            String keyStr = key.toString();
            String keySplit[] = keyStr.split(",");
            Text outWord;
            ArrayListWritable<Text> outsig = new ArrayListWritable<Text>();
            for(String w : keySplit){
                outWord = new Text();
                outWord.set(w);
                outsig.add(outWord);
            }
            IntWritable idxOut = new IntWritable();
            idxOut.set(ct);
            ct++;
            
            output.collect(idxOut, outsig);

        }
    }
    
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
    private static final String OUTPUT = "output";
    private static final String fINPUT = "fin";
    private static final String eINPUT = "ein";

    
    @SuppressWarnings("static-access")
    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("input").create(fINPUT));
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("input").create(eINPUT));
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

        if (!cmdline.hasOption(eVocabSrcOption) || !cmdline.hasOption(fVocabSrcOption) ||
                !cmdline.hasOption(eVocabTgtOption) || !cmdline.hasOption(fVocabTgtOption) ||
                !cmdline.hasOption(e2fProbsOption) || !cmdline.hasOption(f2eProbsOption) ||
                !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(fINPUT) || !cmdline.hasOption(eINPUT) 
                || !cmdline.hasOption(eLangOption) || !cmdline.hasOption(fLangOption) 
                || !cmdline.hasOption(eStopWordsOption) || !cmdline.hasOption(fStopWordsOption) 
                || !cmdline.hasOption(eTokensOption) || !cmdline.hasOption(fTokensOption)
                || !cmdline.hasOption(nSamplesOption)) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        
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
        String fInputPath = cmdline.getOptionValue(fINPUT);
        String eInputPath = cmdline.getOptionValue(eINPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);
        String nSamplesIn = cmdline.getOptionValue(nSamplesOption);

        LOG.info("Tool name: " + this.getClass().getName());
        LOG.info(" - e input file: " + eInputPath);
        LOG.info(" - f input file: " + fInputPath);
        LOG.info(" - output file: " + outputPath);

        JobConf conf = new JobConf(getConf(), SampleSentenceTranslations.class);
        conf.setJobName(String.format("SampleSentencesCLIR[%s: %s]", OUTPUT, outputPath));

        
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
        conf.setLong("rseed", 1123456);
        conf.setInt("nSamples", Integer.parseInt(nSamplesIn));
        //conf.setInt("MINLEN", 5);
        //conf.setInt("MAXLEN", 100);


        conf.setNumMapTasks(4);
        conf.setNumReduceTasks(0);

        
        FileInputFormat.setInputPaths(conf, new Path(eInputPath), new Path(fInputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));

        conf.setMapperClass(SampleMapper.class);
        //conf.setReducerClass(SignatureReducer.class);
        
        //conf.setInputFormat(WikipediaPageInputFormat.class);
        conf.setInputFormat(SequenceFileInputFormat.class);
      //conf.setOutputFormat(SequenceFileOutputFormat.class);
        conf.setOutputFormat(MapFileOutputFormat.class);
        //conf.setOutputFormat(TextOutputFormat.class);
        
        // Set heap space - using old API
        conf.set("mapred.job.map.memory.mb", "4096");
        conf.set("mapred.map.child.java.opts", "-Xmx4096m");
        conf.set("mapred.job.reduce.memory.mb", "6144");
        conf.set("mapred.reduce.child.java.opts", "-Xmx6144m");
        //conf.set("mapred.child.java.opts", "-Xmx2048m");
        
        //conf.setMapOutputKeyClass(Text.class);
        //conf.setMapOutputValueClass(IntWritable.class);
        
        conf.setOutputKeyClass(ArrayListOfLongsWritable.class);
        conf.setOutputValueClass(ArrayListWritable.class);

        // Delete the output directory if it exists already.
        Path outputDir = new Path(outputPath);
        FileSystem.get(conf).delete(outputDir, true);
        
        JobClient.runJob(conf);

        return 0;
    }

    public SampleSentenceTranslations() {}

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new SampleSentenceTranslations(), args);
    }
}
