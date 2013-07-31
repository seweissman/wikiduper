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


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.pair.PairOfInts;

public class JaccardCompare extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(JaccardCompare.class);

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

    
    private static final String matchOutput = "matchesout";
    private static final String nomatchOutput = "nomatchesout";
    private static final String nSamplesOption = "M";

    
    @SuppressWarnings("static-access")
    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("output path").create(matchOutput));
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("output path").create(nomatchOutput));
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


        if (!cmdline.hasOption(matchOutput) || !cmdline.hasOption(nomatchOutput) || !cmdline.hasOption(nSamplesOption)){
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        
        String matchOutputPath = cmdline.getOptionValue(matchOutput);
        String nomatchOutputPath = cmdline.getOptionValue(nomatchOutput);
        String nSamplesIn = cmdline.getOptionValue(nSamplesOption);

        LOG.info("Tool name: " + this.getClass().getName());
        //LOG.info(" - input file: " + inputPath);
        //LOG.info(" - output file: " + outputPath);

        JobConf conf = new JobConf(getConf(), JaccardCompare.class);
        conf.setJobName(String.format("JaccardCompare"));

        
//        FileInputFormat.setInputPaths(conf, new Path(inputPath));
//        FileOutputFormat.setOutputPath(conf, new Path(outputPath));


        int nSentences = 1000;
        int nSamples = Integer.parseInt(nSamplesIn);
        try {
            
            File matchFile = new File(matchOutputPath);
            File nomatchFile = new File(nomatchOutputPath);
            FileOutputStream fosM = null, fosNM = null;
            BufferedWriter dosM = null, dosNM = null;
                
            fosM = new FileOutputStream(matchFile);
            fosNM = new FileOutputStream(nomatchFile);
            dosM = new BufferedWriter(new OutputStreamWriter(fosM));
            dosNM = new BufferedWriter(new OutputStreamWriter(fosNM));
            

            MapFile.Reader id2sentenceReader = new MapFile.Reader(new Path("id2sentence.map/part-00000"), conf);
            HashMap<Integer,ArrayListWritable<Text>> id2sentence = new HashMap<Integer, ArrayListWritable<Text>>();
            IntWritable key = new IntWritable();
            ArrayListWritable<Text> val = new ArrayListWritable<Text>();
            while(id2sentenceReader.next(key, val)){
                id2sentence.put(key.get(), val);
                val = new ArrayListWritable<Text>();
            }

            MapFile.Reader sentence2translationReader = new MapFile.Reader(new Path("sentence2translation.map/part-00000"), conf);
            HashMap<Integer,ArrayListOfIntsWritable> sentence2translation = new HashMap<Integer, ArrayListOfIntsWritable>();
            IntWritable key2 = new IntWritable();
            ArrayListOfIntsWritable val2 = new ArrayListOfIntsWritable();
            while(sentence2translationReader.next(key2, val2)){
                sentence2translation.put(key2.get(), val2);
                val2 = new ArrayListOfIntsWritable();
            }

            MapFile.Reader sentencematchReader = new MapFile.Reader(new Path("sentencematchpairs.map/part-00000"), conf);
            HashSet<PairOfInts> sentencematchpairs = new HashSet<PairOfInts>();
            PairOfInts key3 = new PairOfInts();
            IntWritable val3 = new IntWritable();
            while(sentencematchReader.next(key3, val3)){
                sentencematchpairs.add(key3);
                key3 = new PairOfInts();
            }

            System.out.println("Done reading");
            PairOfInts p = new PairOfInts();
            IntWritable match;
            IntWritable eLineNum = new IntWritable();
            IntWritable eLineId = new IntWritable();
            ArrayListWritable<Text> eSentence = new ArrayListWritable<Text>();
            for(int i=0;i<nSentences;i++){
                System.out.println("eLine " + i);
                //eLineNum.set(2*i);
              ArrayListOfIntsWritable transIdList = sentence2translation.get(2*i);
              //ArrayListOfIntsWritable transIdList = new ArrayListOfIntsWritable();
                //sentence2translationReader.get(eLineNum, transIdList);
               // System.out.println("transIdList " + transIdList);
                for(int j=0;j<nSentences;j++){
                  // System.out.println("fLine " + j);
                  ArrayListWritable<Text> fSentence = id2sentence.get((2*j+1)*nSamples);
                  //ArrayListWritable<Text> fSentence = new ArrayListWritable<Text>();
                    //IntWritable fLineId = new IntWritable();
                    //fLineId.set((2*j+1)*nSamples);
                    //id2sentenceReader.get(fLineId, fSentence);
                   // System.out.println("fLineId " + (2*j+1)*nSamples + " FSentence " + fSentence);
                    float jsimMax = -1.0f;
                    float jsimAvg = 0.0f;
                    for(int id : transIdList){
                        
                        eSentence = id2sentence.get(id);
                        //eLineId.set(id);
                        //id2sentenceReader.get(eLineId, eSentence);
                        float jsim = JaccardSim.jaccardSim(eSentence, fSentence);
                       // System.out.println("\teSentence " + eSentence + " " + jsim);
                        jsimAvg += jsim;
                        if(jsim > jsimMax){
                            jsimMax = jsim;
                        }
                    }
                    jsimAvg = jsimAvg/transIdList.size();
                    if(2*i < 2*j + 1){
                        p.set(2*i, 2*j+1);
                    }else{
                        p.set(2*j+1, 2*i);
                    }
                    //match = new IntWritable();
                    //match = (IntWritable) sentencematchReader.get(p, match);
                    //if(match != null){
                    if(sentencematchpairs.contains(p)){
                        if(jsimMax < .5){
                            System.out.println("Low match: ");
                            System.out.println("\teSentence: " + i + " " + eSentence);
                            System.out.println("\tfSentence: " + j + " " + fSentence);
                        }
                        //System.out.println("match");
                        dosM.write(Float.toString(jsimMax));
                        //dosM.write(Float.toString(jsimAvg));
                        dosM.write("\n");
                    }else{
                        //System.out.println("no match");
                        dosNM.write(Float.toString(jsimMax));
                        //dosNM.write(Float.toString(jsimAvg));
                        dosNM.write("\n");
                    }
                }
            }
            sentencematchReader.close();
            sentence2translationReader.close();
            id2sentenceReader.close();
            dosM.close();
            dosNM.close();
            
        } catch (IOException e2) {
            // TODO Auto-generated catch block
            e2.printStackTrace();
        }

        
        // Delete the output directory if it exists already.
//        Path outputDir = new Path(outputPath);
        //FileSystem.get(conf).delete(outputDir, true);
        
        //JobClient.runJob(conf);

        return 0;
    }

    public JaccardCompare() {}

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new JaccardCompare(), args);
    }
}
