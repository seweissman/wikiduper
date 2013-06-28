package wikiduper.clir;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import wikiduper.application.MergeClusters;
import edu.umd.cloud9.io.pair.PairOfStrings;

public class PreProcessInput extends Configured implements Tool {
        private static final Logger LOG = Logger.getLogger(MergeClusters.class);


        private static final String eFileInOption = "eFile";
        private static final String fFileInOption = "fFile";
        private static final String eLanguageOption = "eLanguage";
        private static final String fLanguageOption = "fLanguage";
        private static final String outFileOption = "outFile";

        @SuppressWarnings("static-access")
        @Override
        public int run(String[] args) throws Exception {
            Options options = new Options();
            options.addOption(OptionBuilder.withArgName("path")
                    .hasArg().withDescription("input path").create(eFileInOption));
            options.addOption(OptionBuilder.withArgName("path")
                    .hasArg().withDescription("input path").create(fFileInOption));
            options.addOption(OptionBuilder.withArgName("path")
                    .hasArg().withDescription("output path").create(outFileOption));
            options.addOption(OptionBuilder.withArgName("en|sv|de|cs|es|zh|ar|tr").hasArg()
                .withDescription("two-letter language code").create(eLanguageOption));
            options.addOption(OptionBuilder.withArgName("num").hasArg()
                    .withDescription("two-letter language code").create(fLanguageOption));

            CommandLine cmdline;
            CommandLineParser parser = new GnuParser();
            try {
                cmdline = parser.parse(options, args);
            } catch (ParseException exp) {
                System.err.println("Error parsing command line: " + exp.getMessage());
                return -1;
            }

            if (!cmdline.hasOption(eFileInOption) || !cmdline.hasOption(fFileInOption)
                    || !cmdline.hasOption(eLanguageOption) || !cmdline.hasOption(fLanguageOption)
                    || !cmdline.hasOption(outFileOption)){
                HelpFormatter formatter = new HelpFormatter();
                formatter.setWidth(120);
                formatter.printHelp(this.getClass().getName(), options);
                ToolRunner.printGenericCommandUsage(System.out);
                return -1;
            }
            

            String eReadFile = cmdline.getOptionValue(eFileInOption);
            String fReadFile = cmdline.getOptionValue(fFileInOption);
            String outFile = cmdline.getOptionValue(outFileOption);
            String eLang = cmdline.getOptionValue(eLanguageOption);
            String fLang = cmdline.getOptionValue(fLanguageOption);
            //FileSystem localFs = null;
            File eFile = new File(eReadFile);
            File fFile = new File(fReadFile);
            JobConf conf = new JobConf(getConf(), PreProcessInput.class);
            
            try {
                //localFs = FileSystem.getLocal(conf);
                
                FileInputStream fis1 = null, fis2 = null;
                BufferedReader dis1 = null, dis2 = null;
                fis1 = new FileInputStream(eFile);
                fis2 = new FileInputStream(fFile);
                dis1 = new BufferedReader(new InputStreamReader(fis1, "UTF-8"));
                dis2 = new BufferedReader(new InputStreamReader(fis2, "UTF-8"));

                SequenceFile.Writer writer = SequenceFile.createWriter(conf, 
                        SequenceFile.Writer.file(new Path(outFile)), 
                        SequenceFile.Writer.keyClass(IntWritable.class), 
                        SequenceFile.Writer.valueClass(PairOfStrings.class));
                
                IntWritable key = new IntWritable();
                PairOfStrings langLine = new PairOfStrings();
                String eLine;
                String fLine;
                int ct = 0;
                while((eLine = dis1.readLine()) != null){
                    eLine = eLine.trim();
                    langLine.set(eLang, eLine);
                    key.set(ct);
                    writer.append(key, langLine);
                    ct++;
                    fLine = dis2.readLine();
                    fLine = fLine.trim();
                    langLine.set(fLang, fLine);
                    key.set(ct);
                    writer.append(key, langLine);
                    ct++;
                }
                    
                writer.close();
                dis1.close();
                dis2.close();

            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
            return 0;
        }
        public PreProcessInput() {}

        public static void main(String[] args) throws Exception {
            ToolRunner.run(new PreProcessInput(), args);
        }

    
}
