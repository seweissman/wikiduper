package wikiduper.clir.minhash;

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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.pair.PairOfLongInt;
import edu.umd.cloud9.io.pair.PairOfStrings;

public class PreprocessTextInput extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(PreprocessTextInput.class);

    private static class LanguageMapper extends MapReduceBase implements
    Mapper<LongWritable, Text, PairOfLongInt, PairOfStrings> {
        
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
        
        
           public void map(LongWritable key, Text p, OutputCollector<PairOfLongInt, PairOfStrings> output,
                    Reporter reporter) throws IOException {
               
               
               String content = p.toString();
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
                        docIdSentenceCt.set(key.get(),sentencect);
                        //System.out.println("SENTENCE: " + langSentence.toString());
                        output.collect(docIdSentenceCt, langSentence);
                        sentencect++;
                    }
            
                }catch(Throwable e){
                    System.err.println("WARNING: Possible stack overflow from regex at key " + key);
                //System.err.println("WARNING: Possible stack overflow from regex at docid " + p.getDocid() + " and sentence # " + p.toString());
                }
            }
        }

        
        
        
        public void configure(JobConf job) {
            lang = job.get("doc.language", "en");
        }
    }

 
    private static final String eINPUT = "ein";
    private static final String fINPUT = "fin";
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

        JobConf conf = new JobConf(getConf(), PreprocessTextInput.class);
        conf.setJobName(String.format("PreprocessDocInput[%s: %s, %s: %s, %s: %s]", eINPUT, eInputPath, fINPUT, fInputPath, eOUTPUT, eOutputPath,
                eLANGUAGE_OPTION, eLanguage, fLANGUAGE_OPTION, fLanguage));

        conf.setNumMapTasks(4);
        conf.setNumReduceTasks(0);

        conf.setMapperClass(LanguageMapper.class);
        
        //conf.setInputFormat(WikipediaPageInputFormat.class);
        conf.setInputFormat(TextInputFormat.class);
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
        
        conf.set("doc.language", eLanguage);

        // Delete the output directory if it exists already.
        fs.delete(ePath, true);

        JobClient.runJob(conf);

        
        // Job 2

        FileInputFormat.setInputPaths(conf, new Path(fInputPath));
        FileOutputFormat.setOutputPath(conf, fPath);
        
        conf.set("doc.language", fLanguage);

        // Delete the output directory if it exists already.
        fs.delete(fPath, true);
        
        JobClient.runJob(conf);

        
        return 0;
    }

    public PreprocessTextInput() {}

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new PreprocessTextInput(), args);
    }
}
