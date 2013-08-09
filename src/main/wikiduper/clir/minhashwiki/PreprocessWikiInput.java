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


import java.io.IOException;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
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
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.wikiclean.WikiClean;
import org.wikiclean.WikiClean.WikiLanguage;
import org.wikiclean.WikiCleanBuilder;

import wikiduper.wikipedia.WikipediaPage;
import edu.umd.cloud9.io.array.ArrayListOfLongsWritable;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.io.pair.PairOfLongInt;
import edu.umd.cloud9.io.pair.PairOfStringInt;
import edu.umd.cloud9.io.pair.PairOfStrings;

public class PreprocessWikiInput extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(PreprocessWikiInput.class);

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

    private static enum PageTypes {
        TOTAL, REDIRECT, DISAMBIGUATION, EMPTY, ARTICLE, STUB, NON_ARTICLE
    };
    
    private static class LanguageMapper extends MapReduceBase implements
    Mapper<IntWritable, WikipediaPage, PairOfLongInt, PairOfStrings> {
    //Mapper<LongWritable, WikipediaPage, ArrayListOfLongsWritable, PairOfStringInt> {
        
        static String lang;
        //Adapted from http://stackoverflow.com/questions/5553410/regular-expression-match-a-sentence
        static final Pattern sentenceregex = Pattern.compile(
                "# Match a sentence ending in punctuation or EOS.\n" +
                        "[\\s]*    # Leading white space\n" + 
                        "([A-Z\"]    # First char capital letter or quotation\n" +
                        "[^.!?\\n]*      # Greedily consume up to punctuation.\n" +
                        "(?:          # Group for unrolling the loop.\n" +
                        "  [.!?]      # (special) inner punctuation ok if\n" +
                        "  (?!['\"]?\\s|$)  # not followed by ws or EOS.\n" +
                        "  [^.!?]*    # Greedily consume up to punctuation.\n" +
                        ")*           # Zero or more (special normal*)\n" +
                        "[.!?]?       # Optional ending punctuation.\n" +
                        "['\"]?)       # Optional closing quote.\n" +
                        "\\s*       # Trailing white space or new line\n",
                        Pattern.MULTILINE | Pattern.COMMENTS);
        
        
        //public void map(LongWritable key, WikipediaPage p, OutputCollector<ArrayListOfLongsWritable, PairOfStringInt> output,
          //      Reporter reporter) throws IOException {
        
        public static WikiClean cleaner;
        
           public void map(IntWritable key, WikipediaPage p, OutputCollector<PairOfLongInt, PairOfStrings> output,
                    Reporter reporter) throws IOException {
               
               
            if (p.isRedirect()) {
                reporter.incrCounter(PageTypes.REDIRECT, 1);

            } else if (p.isDisambiguation()) {
                reporter.incrCounter(PageTypes.DISAMBIGUATION, 1);
            } else if (p.isEmpty()) {
                reporter.incrCounter(PageTypes.EMPTY, 1);
            } else if (p.isArticle()) {
                reporter.incrCounter(PageTypes.ARTICLE, 1);

                if (p.isStub()) {
                    reporter.incrCounter(PageTypes.STUB, 1);
                }
            } else {
                reporter.incrCounter(PageTypes.NON_ARTICLE, 1);
            }
            
            if(!p.isArticle() || p.isEmpty()) return;
            String raw = p.getRawXML();
            String content = cleaner.clean(raw);
            //String title = cleaner.getTitle(content);
            //System.out.println(lang + " " + key + " TITLE = " + p.getTitle());
            if(content == null) return;
            if(p.getDocid() == null) return;
            long id = Long.parseLong(p.getDocid());
            String cleancontent = content
                    //.replace("\n", " ")
                    .replace("  ", " ")
                    .replace(",","")
                    .replace("(b.", "(b")
                    .replace("(d.", "(d");
            
            String lines[] = cleancontent.split("\n");
            Matcher m;
            PairOfStrings langSentence;
            PairOfLongInt docIdSentenceCt;
            int sentencect = 0;
            for(String line: lines){
                //System.out.println(p.getDocid() + "\n>>>>>>>>CONTENT\n" + line + "\nCONTENT<<<<<<<<<<\n");
                m = sentenceregex.matcher(line);

                // Assume a whole Wikipedia article has been passed to the mapper; track sentence number by counting
                try{
                    //if(!m.matches()) continue;
                    // For each sentence in the input text:
                    while(m.find()){
                        langSentence = new PairOfStrings();
                        docIdSentenceCt = new PairOfLongInt();
                        String sentence = m.group(1);
                        langSentence.set(lang, sentence);
                        //docIdSentenceCt.set(key.get(),sentencect);
                        docIdSentenceCt.set(id,sentencect);
                        output.collect(docIdSentenceCt, langSentence);
                        sentencect++;
                    }
            
                }catch(Throwable e){
                    System.err.println("WARNING: Possible stack overflow from regex at docid " + p.getDocid());
                //System.err.println("WARNING: Possible stack overflow from regex at docid " + p.getDocid() + " and sentence # " + p.toString());
                }
            }
        }

        
        
        
        public void configure(JobConf job) {
            
            lang = job.get("wiki.language", "en");
            WikiLanguage wikilang = WikiLanguage.valueOf(lang.toUpperCase());
            cleaner =  new WikiCleanBuilder()
                        .withLanguage(wikilang)
                        .withTitle(true)
                        .withFooter(false).build();

        }
    }

 
    private static final String eINPUT = "ewiki";
    private static final String fINPUT = "fwiki";
    private static final String eOUTPUT = "eout";
    private static final String fOUTPUT = "fout";
    private static final String eLANGUAGE_OPTION = "elang";
    private static final String fLANGUAGE_OPTION = "flang";
    
    @SuppressWarnings("static-access")
    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("bz2 input path").create(fINPUT));
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("bz2 input path").create(eINPUT));
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("output path").create(eOUTPUT));
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("output path").create(fOUTPUT));
        options.addOption(OptionBuilder.withArgName("en|sv|de|cs|es|zh|ar|tr").hasArg()
                .withDescription("two-letter language code").create(eLANGUAGE_OPTION));
        options.addOption(OptionBuilder.withArgName("en|sv|de|cs|es|zh|ar|tr").hasArg()
                .withDescription("two-letter language code").create(fLANGUAGE_OPTION));
        
        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();
        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(eINPUT) || !cmdline.hasOption(fINPUT) 
                || !cmdline.hasOption(eLANGUAGE_OPTION) || !cmdline.hasOption(fLANGUAGE_OPTION) 
                || !cmdline.hasOption(eOUTPUT) || !cmdline.hasOption(fOUTPUT)){
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String eInputPath = cmdline.getOptionValue(eINPUT);
        String fInputPath = cmdline.getOptionValue(fINPUT);
        String eOutputPath = cmdline.getOptionValue(eOUTPUT);
        String fOutputPath = cmdline.getOptionValue(fOUTPUT);
        String eLanguage = cmdline.getOptionValue(eLANGUAGE_OPTION);
        String fLanguage = cmdline.getOptionValue(fLANGUAGE_OPTION);
        

        LOG.info("Tool name: " + this.getClass().getName());
        LOG.info(" - e input file: " + eInputPath);
        LOG.info(" - f input file: " + fInputPath);
        LOG.info(" - e output file: " + eOutputPath);
        LOG.info(" - f output file: " + fOutputPath);
        LOG.info(" - e language: " + eLanguage);
        LOG.info(" - f language: " + fLanguage);

        JobConf conf = new JobConf(getConf(), PreprocessWikiInput.class);
        conf.setJobName(String.format("PreprocessWikiInput[%s: %s, %s: %s, %s: %s]", eINPUT, eInputPath, fINPUT, fInputPath, eOUTPUT, eOutputPath,
                eLANGUAGE_OPTION, eLanguage, fLANGUAGE_OPTION, fLanguage));

        conf.setNumMapTasks(4);
        conf.setNumReduceTasks(0);

        conf.setMapperClass(LanguageMapper.class);
        
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
        
        conf.setOutputKeyClass(PairOfLongInt.class);
        conf.setOutputValueClass(PairOfStrings.class);
        
        FileSystem fs = FileSystem.get(conf);        
        Path ePath = new Path(eOutputPath);
        Path fPath = new Path(fOutputPath);
        
        // Job 1
        FileInputFormat.setInputPaths(conf, new Path(eInputPath));
        FileOutputFormat.setOutputPath(conf, ePath);
        
        conf.set("wiki.language", eLanguage);

        // Delete the output directory if it exists already.
        fs.delete(ePath, true);

        JobClient.runJob(conf);

        
        // Job 2

        FileInputFormat.setInputPaths(conf, new Path(fInputPath));
        FileOutputFormat.setOutputPath(conf, fPath);
        
        conf.set("wiki.language", fLanguage);

        // Delete the output directory if it exists already.
        fs.delete(fPath, true);
        
        JobClient.runJob(conf);

        
        return 0;
    }

    public PreprocessWikiInput() {}

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new PreprocessWikiInput(), args);
    }
}
