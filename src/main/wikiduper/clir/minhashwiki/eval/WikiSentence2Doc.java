package wikiduper.clir.minhashwiki.eval;

import java.io.IOException;
import java.util.Iterator;
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
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.wikiclean.WikiClean;
import org.wikiclean.WikiClean.WikiLanguage;
import org.wikiclean.WikiCleanBuilder;

import edu.umd.cloud9.io.pair.PairOfLongs;
import edu.umd.cloud9.io.pair.PairOfStrings;

import wikiduper.utils.DocSentence;
import wikiduper.wikipedia.WikipediaPage;


public class WikiSentence2Doc extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(WikiSentence2Doc.class);
    
  private static enum PageTypes {
        TOTAL, REDIRECT, DISAMBIGUATION, EMPTY, ARTICLE, STUB, NON_ARTICLE
    };
    
    private static class LanguageMapper extends MapReduceBase implements
    Mapper<IntWritable, WikipediaPage, DocSentence, PairOfStrings> {
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
        
        
        public static WikiClean cleaner;
        
           public void map(IntWritable key, WikipediaPage p, OutputCollector<DocSentence, PairOfStrings> output,
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
            String title = p.getTitle();
            //System.out.println(lang + " " + key + " TITLE = " + p.getTitle());
            if(content == null) return;
            if(p.getDocid() == null) return;
            String cleancontent = content
                    //.replace("\n", " ")
                    .replace("  ", " ")
                    .replace(",","")
                    .replace("(b.", "(b")
                    .replace("(d.", "(d");
            
            String lines[] = cleancontent.split("\n");
            Matcher m;
            
            int sentencect = 0;
            if(Long.parseLong(p.getDocid()) > 100) return;
            for(String line: lines){
                //System.out.println(p.getDocid() + "\n>>>>>>>>CONTENT\n" + line + "\nCONTENT<<<<<<<<<<\n");
                m = sentenceregex.matcher(line);

                // Assume a whole Wikipedia article has been passed to the mapper; track sentence number by counting
                //try{
                    //if(!m.matches()) continue;
                    // For each sentence in the input text:
                    while(m.find()){
                        String sentence = m.group(1);
                        //if (p.getTitle().length() <= 0.3*p.getContent().length()) {
                            DocSentence ds = new DocSentence();
                            ds.setId(Long.valueOf(p.getDocid()));
                            ds.setSentence(sentencect);
                            ds.setLanguage(lang);
                            PairOfStrings titlesentence = new PairOfStrings();
                            titlesentence.set(title, sentence);
                             output.collect(ds,titlesentence);
                             sentencect++;
                        //}
                    }
            
               /* }catch(Throwable e){
                    System.err.println(e.toString());
                    e.printStackTrace();
                    System.err.println("WARNING: Possible stack overflow from regex at docid " + p.getDocid());
                //System.err.println("WARNING: Possible stack overflow from regex at docid " + p.getDocid() + " and sentence # " + p.toString());
                }
                */
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
    
    private static class IDMapper2 extends MapReduceBase implements
    Mapper<IntWritable, WikipediaPage, DocSentence, Text> {
        static String lang;
        public static Text ONE = new Text("1");
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
        
        
        public static WikiClean cleaner;
        
           public void map(IntWritable key, WikipediaPage p, OutputCollector<DocSentence, Text> output,
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
            String title = p.getTitle();
            //System.out.println(lang + " " + key + " TITLE = " + p.getTitle());
            if(content == null) return;
            if(p.getDocid() == null) return;
            String cleancontent = content
                    //.replace("\n", " ")
                    .replace("  ", " ")
                    .replace(",","")
                    .replace("(b.", "(b")
                    .replace("(d.", "(d");
            
            String lines[] = cleancontent.split("\n");
            Matcher m;
            
            int sentencect = 0;
            if(Long.parseLong(p.getDocid()) > 100) return;
            for(String line: lines){
                //System.out.println(p.getDocid() + "\n>>>>>>>>CONTENT\n" + line + "\nCONTENT<<<<<<<<<<\n");
                m = sentenceregex.matcher(line);

                // Assume a whole Wikipedia article has been passed to the mapper; track sentence number by counting
                //try{
                    //if(!m.matches()) continue;
                    // For each sentence in the input text:
                    while(m.find()){
                        String sentence = m.group(1);
                        //if (p.getTitle().length() <= 0.3*p.getContent().length()) {
                            DocSentence ds = new DocSentence();
                            ds.setId(Long.valueOf(p.getDocid()));
                            ds.setSentence(sentencect);
                            ds.setLanguage(lang);
                             output.collect(ds,ONE);
                             sentencect++;
                        //}
                    }
            
               /* }catch(Throwable e){
                    System.err.println(e.toString());
                    e.printStackTrace();
                    System.err.println("WARNING: Possible stack overflow from regex at docid " + p.getDocid());
                //System.err.println("WARNING: Possible stack overflow from regex at docid " + p.getDocid() + " and sentence # " + p.toString());
                }
                */
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
    
    private static class IDMapper extends MapReduceBase implements
//    Mapper<IntWritable, WikipediaPage, DocSentence, IntWritable> {
        Mapper<IntWritable, WikipediaPage, PairOfLongs, IntWritable> {

        
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
        
        
        public static WikiClean cleaner;
        public static IntWritable ONE = new IntWritable(1);
        //public void map(IntWritable key, WikipediaPage p, OutputCollector<DocSentence, IntWritable> output,
          //      Reporter reporter) throws IOException {
        public void map(IntWritable key, WikipediaPage p, OutputCollector<PairOfLongs, IntWritable> output,
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
            String title = p.getTitle();
            //System.out.println(lang + " " + key + " TITLE = " + p.getTitle());
            if(content == null) return;
            if(p.getDocid() == null) return;
            String cleancontent = content
                    //.replace("\n", " ")
                    .replace("  ", " ")
                    .replace(",","")
                    .replace("(b.", "(b")
                    .replace("(d.", "(d");
            
            String lines[] = cleancontent.split("\n");
            Matcher m;
            
            int sentencect = 0;
            if(Long.parseLong(p.getDocid()) > 100) return;
            for(String line: lines){
                //System.out.println(p.getDocid() + "\n>>>>>>>>CONTENT\n" + line + "\nCONTENT<<<<<<<<<<\n");
                m = sentenceregex.matcher(line);

                // Assume a whole Wikipedia article has been passed to the mapper; track sentence number by counting
                //try{
                    //if(!m.matches()) continue;
                    // For each sentence in the input text:
                    while(m.find()){
                        String sentence = m.group(1);
                        //if (p.getTitle().length() <= 0.3*p.getContent().length()) {
                            //DocSentence ds = new DocSentence();
                        PairOfLongs ds = new PairOfLongs();
                        ds.set(Long.valueOf(p.getDocid()), sentencect);
                            //ds.setId(Long.valueOf(p.getDocid()));
                            //ds.setSentence(sentencect);
                            //ds.setLanguage(lang);
                             output.collect(ds,ONE);
                             sentencect++;
                        //}
                    }
            
               /* }catch(Throwable e){
                    System.err.println(e.toString());
                    e.printStackTrace();
                    System.err.println("WARNING: Possible stack overflow from regex at docid " + p.getDocid());
                //System.err.println("WARNING: Possible stack overflow from regex at docid " + p.getDocid() + " and sentence # " + p.toString());
                }
                */
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
    
    
    
    private static class LanguageReducer extends MapReduceBase implements
        Reducer<DocSentence, PairOfStrings, Text, Text> {
        static int partition;
        static int id=1;
        static final Text titleOut = new Text();
        static final Text pageOut = new Text();
        
           public void reduce(DocSentence ds, Iterator<PairOfStrings> values, OutputCollector<Text, Text> output,
                    Reporter reporter) throws IOException {
               
               while(values.hasNext()){
                   PairOfStrings titlesentence = values.next();
                   String sentence = titlesentence.getRightElement();
                   String title = titlesentence.getLeftElement();
                   titleOut.set(title);
                   String xmlPage;
                   xmlPage = makePageText(sentence,title.toString(),20*id + (partition % 20));
                   pageOut.set(xmlPage);
                   output.collect(titleOut,pageOut);
                   id++;
                 }
            }
        
        public static String makePageText(String line,String title,long id){
            String text = "<page>"
                    +"<title>"
                    + title 
                    + "</title>"
                    + "<ns>0</ns>"
                    + "<id>"
                    + id
                    + "</id>"
                    + "<revision>"
                    + "<id>560581215</id>"
                    + "<parentid>560575666</parentid>"
                    + "<timestamp>2013-06-19T09:39:33Z</timestamp>"
                    + "<contributor>"
                    + "<username>Jerzy</username>"
                    + "<id>21860</id>"
                    + "</contributor>"
                    + "<comment>nothing</comment>"
                    + "<text xml:space=\"preserve\">"
                    + line
                    + "</text>"
                    + "<sha1>04cbg4kl87va6z9u5w5eepb2jtagilz</sha1>"
                    + "<model>wikitext</model>"
                    + "<format>text/x-wiki</format>"
                    + "</revision>"
                    + "</page>";
            return text;
        }
        
        public void configure(JobConf job) {
            partition = job.getInt("mapred.task.partition",0);
          } 
        
    }
    
    private static class IDReducer2 extends MapReduceBase implements
    Reducer<DocSentence, Text, Text, Text> {
    static int partition;
    static int id=1;
    static final Text titleOut = new Text();
    static final Text pageOut = new Text();
    
       public void reduce(DocSentence ds, Iterator<Text> values, OutputCollector<Text, Text> output,
                Reporter reporter) throws IOException {
           
           while(values.hasNext()){
               //PairOfStrings titlesentence = values.next();
               //String sentence = titlesentence.getRightElement();
               //String title = titlesentence.getLeftElement();
               //titleOut.set(title);
               titleOut.set(Integer.toString(id));
               //String xmlPage;
               //xmlPage = makePageText(sentence,title.toString(),20*id + (partition % 20));
               pageOut.set(ds.getId()+","+ds.getSentence());
               output.collect(titleOut,pageOut);
               id++;
             }
        }
    
    public static String makePageText(String line,String title,long id){
        String text = "<page>"
                +"<title>"
                + title 
                + "</title>"
                + "<ns>0</ns>"
                + "<id>"
                + id
                + "</id>"
                + "<revision>"
                + "<id>560581215</id>"
                + "<parentid>560575666</parentid>"
                + "<timestamp>2013-06-19T09:39:33Z</timestamp>"
                + "<contributor>"
                + "<username>Jerzy</username>"
                + "<id>21860</id>"
                + "</contributor>"
                + "<comment>nothing</comment>"
                + "<text xml:space=\"preserve\">"
                + line
                + "</text>"
                + "<sha1>04cbg4kl87va6z9u5w5eepb2jtagilz</sha1>"
                + "<model>wikitext</model>"
                + "<format>text/x-wiki</format>"
                + "</revision>"
                + "</page>";
        return text;
    }
    
    public void configure(JobConf job) {
        partition = job.getInt("mapred.task.partition",0);
      } 
    
}

    
    private static class IDReducer extends MapReduceBase implements
    //Reducer<DocSentence, IntWritable, IntWritable, DocSentence> {
    Reducer<PairOfLongs, IntWritable, IntWritable, PairOfLongs> {

    static int id=1;
    static IntWritable idOut = new IntWritable();
    //static DocSentence dsOut = new DocSentence();
    static PairOfLongs dsOut = new PairOfLongs();
    //static int partition;
    //public void reduce(DocSentence ds, Iterator<IntWritable> values, OutputCollector<IntWritable, DocSentence> output,
      //      Reporter reporter) throws IOException {
        public void reduce(PairOfLongs ds, Iterator<IntWritable> values, OutputCollector<IntWritable, PairOfLongs> output,
                Reporter reporter) throws IOException {
           
           while(values.hasNext()){
               //idOut.set(20*id + (partition % 20));
               idOut.set(id);
               dsOut.set(ds.getLeftElement(), ds.getRightElement());
               //dsOut.setId(ds.getId());
               //dsOut.setLanguage(ds.getLanguage());
               //dsOut.setSentence(ds.getSentence());
               output.collect(idOut,dsOut);
               id++;
             }
        }
       
       //public void configure(JobConf job) {
         //  partition = job.getInt("mapred.task.partition",0);
         //} 
    }    
    
    private static final String eINPUT = "ewiki";
    private static final String fINPUT = "fwiki";
    private static final String eOUTPUT = "eout";
    private static final String fOUTPUT = "fout";
    private static final String eMAP = "emap";
    private static final String fMAP = "fmap";
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
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("map output path").create(eMAP));
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("map output path").create(fMAP));
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
        String eMapOutputPath = cmdline.getOptionValue(eMAP);
        String fMapOutputPath = cmdline.getOptionValue(fMAP);
        String eLanguage = cmdline.getOptionValue(eLANGUAGE_OPTION);
        String fLanguage = cmdline.getOptionValue(fLANGUAGE_OPTION);
        

        LOG.info("Tool name: " + this.getClass().getName());
        LOG.info(" - e input file: " + eInputPath);
        LOG.info(" - f input file: " + fInputPath);
        LOG.info(" - e output file: " + eOutputPath);
        LOG.info(" - f output file: " + fOutputPath);
        LOG.info(" - e language: " + eLanguage);
        LOG.info(" - f language: " + fLanguage);

        JobConf conf = new JobConf(getConf(), WikiSentence2Doc.class);
        conf.setJobName(String.format("WikiSentence2Doc[%s: %s, %s: %s, %s: %s, %s: %s]", eINPUT, eInputPath, fINPUT, fInputPath, eOUTPUT, eOutputPath,
                fOUTPUT, fOutputPath, eLANGUAGE_OPTION, eLanguage, fLANGUAGE_OPTION, fLanguage));

        conf.setNumMapTasks(10);
        conf.setNumReduceTasks(4);

        //conf.setMapOutputKeyClass(DocSentence.class);
        //conf.setMapOutputValueClass(IntWritable.class);
        //conf.setOutputKeyClass(IntWritable.class);
        //conf.setOutputValueClass(DocSentence.class);
        
        //conf.setMapOutputKeyClass(PairOfLongs.class);
        //conf.setMapOutputValueClass(IntWritable.class);
        //conf.setOutputKeyClass(IntWritable.class);
        //conf.setOutputValueClass(PairOfLongs.class);
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapOutputKeyClass(DocSentence.class);
        conf.setMapOutputValueClass(Text.class);
        
        //conf.setMapperClass(LanguageMapper.class);
        //conf.setReducerClass(LanguageReducer.class);
        
        //conf.setInputFormat(WikipediaPageInputFormat.class);
        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        //conf.setOutputFormat(TextOutputFormat.class);
        
        // Set heap space - using old API
        conf.set("mapred.job.map.memory.mb", "4096");
        conf.set("mapred.map.child.java.opts", "-Xmx4096m");
        conf.set("mapred.job.reduce.memory.mb", "4096");
        conf.set("mapred.reduce.child.java.opts", "-Xmx4096m");
        //conf.set("mapred.child.java.opts", "-Xmx2048m");


        FileSystem fs = FileSystem.get(conf);        
        Path eOutPath = new Path(eOutputPath);
        Path fOutPath = new Path(fOutputPath);
        Path eMapOutPath = new Path(eMapOutputPath);
        Path fMapOutPath = new Path(fMapOutputPath);
        
        // Job 1
        FileInputFormat.setInputPaths(conf, new Path(eInputPath));
        FileOutputFormat.setOutputPath(conf, eOutPath);
        
        conf.set("wiki.language", eLanguage);

        // Delete the output directory if it exists already.
        fs.delete(eOutPath, true);

        //JobClient.runJob(conf);

        
        // Job 2

        FileInputFormat.setInputPaths(conf, new Path(fInputPath));
        FileOutputFormat.setOutputPath(conf, fOutPath);
        
        conf.set("wiki.language", fLanguage);

        // Delete the output directory if it exists already.
        fs.delete(fOutPath, true);
        
        //JobClient.runJob(conf);

        //conf = new JobConf(getConf(), WikiSentence2Doc.class);
        conf.setJobName(String.format("WikiSentence2DocMap[%s: %s, %s: %s, %s: %s, %s: %s]", eINPUT, eInputPath, fINPUT, fInputPath, eOUTPUT, eOutputPath,
                fOUTPUT, fOutputPath, eLANGUAGE_OPTION, eLanguage, fLANGUAGE_OPTION, fLanguage));

        conf.setMapperClass(IDMapper2.class);
        conf.setReducerClass(IDReducer2.class);
        
        //conf.setInputFormat(SequenceFileInputFormat.class);
        //conf.setOutputFormat(SequenceFileOutputFormat.class);
        
        // Set heap space - using old API
        //conf.set("mapred.job.map.memory.mb", "6144");
        //conf.set("mapred.map.child.java.opts", "-Xmx6144m");
        //conf.set("mapred.job.reduce.memory.mb", "6144");
        //conf.set("mapred.reduce.child.java.opts", "-Xmx6144m");

        //conf.setOutputKeyClass(IntWritable.class);
        //conf.setOutputValueClass(DocSentence.class);
        

        // Job 3
        /*
        FileInputFormat.setInputPaths(conf, new Path(eInputPath));
        FileOutputFormat.setOutputPath(conf, eMapOutPath);
        
        conf.set("wiki.language", eLanguage);

        // Delete the output directory if it exists already.
        fs.delete(eMapOutPath, true);

        JobClient.runJob(conf);
        */
        
        // Job 4

        FileInputFormat.setInputPaths(conf, new Path(fInputPath));
        FileOutputFormat.setOutputPath(conf, fMapOutPath);
        
        conf.set("wiki.language", fLanguage);

        // Delete the output directory if it exists already.
        fs.delete(fMapOutPath, true);
        
        JobClient.runJob(conf);
        
        return 0;
    }

    public WikiSentence2Doc() {}

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new WikiSentence2Doc(), args);
    }
}
