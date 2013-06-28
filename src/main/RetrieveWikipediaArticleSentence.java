import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import wikiduper.wiki.WikipediaPagesBz2InputStream;
import wikiduper.wikipedia.WikipediaPage;
import wikiduper.wikipedia.language.WikipediaPageFactory;


/**
 * Tool for providing command-line access to sentences in wikipedia articles given a docid. This does
 * not run as a MapReduce job.
 * 
 * Here's a sample invocation:
 * hadoop jar MyJAR.jar RetrieveWikipediaArticleSentence -input some-wiki-dump.xml.bz2 -docID 5 -sentenceID 6
 * 
 * will print out the 6th sentence in the wikipedia article whose docID is 5
 * 
 * Note, you'll have to build a jar that contains the contents of bliki-core-3.0.15.jar and
 * commons-lang-2.5.jar, since -libjars won't work for this program (since it's not a MapReduce
 * job).
 * 
 * @author Joshua Bradley
 */
public class RetrieveWikipediaArticleSentence extends Configured implements Tool {

    private static final String INPUT = "input";
    private static final String DOCID = "docID";
    private static final String SENTENCEID = "sentenceID";

    static final Pattern sentenceregex = Pattern.compile(
            "# Match a sentence ending in punctuation or EOS.\n" +
                    "[\\s]*    # Leading white space\n" + 
                    "([A-Z\"]    # First char capital letter or quotation\n" +
                    "[^.!?]*      # Greedily consume up to punctuation.\n" +
                    "(?:          # Group for unrolling the loop.\n" +
                    "  [.!?]      # (special) inner punctuation ok if\n" +
                    "  (?!['\"]?\\s|$)  # not followed by ws or EOS.\n" +
                    "  [^.!?]*    # Greedily consume up to punctuation.\n" +
                    ")*           # Zero or more (special normal*)\n" +
                    "[.!?]?       # Optional ending punctuation.\n" +
                    "['\"]?)       # Optional closing quote.\n" +
                    "\\s*$?       # Trailing white space\n",
                    Pattern.MULTILINE | Pattern.COMMENTS);

    @SuppressWarnings("static-access")
    public int run(String[] args) throws Exception {

        // check command line arguments
        Options options = new Options();
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("bz2 input path").create(INPUT));
        options.addOption(OptionBuilder.withArgName("num")
                .hasArg().withDescription("article ID").create(DOCID));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("sentence ID").create(SENTENCEID));

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();
        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(DOCID) || !cmdline.hasOption(SENTENCEID)) {
            System.out.println("Not all arguments specified");
            System.out.println("Given args: " + Arrays.toString(args));
            System.out.println("Correct usage: -input [wiki-dump-path] -docID [wiki-article-docID] -sentenceID [wiki-article-sentenceID]");
            return -1;
        }

        // grab the command line arguments
        String input = cmdline.getOptionValue(INPUT);
        String docID = cmdline.getOptionValue(DOCID);
        int sentID = Integer.parseInt(cmdline.getOptionValue(SENTENCEID));
        
        // check for invalid Wikipedia article ID
        if (Integer.parseInt(docID) < 0) {
            System.out.println("ERROR: Invalid wikipedia article ID parameter.");
            return -1;
        }
        
        // check for invalid sentence ID
        if (sentID < 0) {
            System.out.println("ERROR: Invalid sentence ID parameter.");
            return -1;
        }
        
        
        // Open up stream to the wiki dump
        WikipediaPage p = WikipediaPageFactory.createWikipediaPage("en");
        WikipediaPagesBz2InputStream stream = new WikipediaPagesBz2InputStream(input);

        // search the wiki dump for the correct article
        while (stream.readNext(p)){

            if (p.getDocid().equals(docID)) {

                int sentenceCount = 0;
                String content = p.getContent();
                if(content == null) continue;
                String line = content
                        .replace("\n", " ")
                        .replace("  ", " ")
                        .replace(",","")
                        .replace("(b.", "(b")
                        .replace("(d.", "(d");
                Matcher m = sentenceregex.matcher(line);

                // iterate to the correct sentence
                while(m.find()){
                    if (sentenceCount == sentID) {
                        String sentence = m.group(1);
                        // print blank lines to help differentiate output from auto-generated warning messages
                        System.out.println("\n" + sentence + "\n");
                        return 0;
                    }
                    sentenceCount++;
                }
            }
        }
        
        // print blank lines to help differentiate output from auto-generated warning messages
        System.out.println("\nDid not find specified article " + docID + " or sentence " + sentID + " in " + input + "\n");
        return 0;
    }

    private RetrieveWikipediaArticleSentence() {}

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new RetrieveWikipediaArticleSentence(), args);
    }
}
