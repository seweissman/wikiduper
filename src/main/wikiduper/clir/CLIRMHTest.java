package wikiduper.clir;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import ivory.core.tokenize.Tokenizer;
import ivory.core.tokenize.TokenizerFactory;
import edu.umd.cloud9.io.map.HMapSIW;
import edu.umd.hooka.Vocab;
import edu.umd.hooka.alignment.HadoopAlign;
import edu.umd.hooka.ttables.TTable_monolithic_IFAs;

public class CLIRMHTest {

    private static void readSentences(String eReadFile, String fReadFile, String eLang, String fLang,
            Vocab eVocabSrc, Vocab eVocabTgt, Vocab fVocabSrc, Vocab fVocabTgt, String eToken, String fToken, String eStopwordsFile, String fStopwordsFile,
            TTable_monolithic_IFAs e2fProbs, TTable_monolithic_IFAs f2eProbs) throws IOException,
            ClassNotFoundException, InstantiationException, IllegalAccessException {
          File eFile = new File(eReadFile);
          File fFile = new File(fReadFile);
          
        Tokenizer eTokenizer = TokenizerFactory.createTokenizer(eLang, eToken, true, eStopwordsFile, eStopwordsFile + ".stemmed", null);
        Tokenizer fTokenizer = TokenizerFactory.createTokenizer(fLang, fToken, true, fStopwordsFile, fStopwordsFile + ".stemmed", null);

        FileInputStream fis1 = null, fis2 = null;
        BufferedReader dis1 = null, dis2 = null;
        try {
        fis1 = new FileInputStream(eFile);
        fis2 = new FileInputStream(fFile);
        dis1 = new BufferedReader(new InputStreamReader(fis1, "UTF-8"));
        dis2 = new BufferedReader(new InputStreamReader(fis2, "UTF-8"));
        HMapSIW fSent = new HMapSIW();
        HMapSIW eSent = new HMapSIW();
        String eLine = null, fLine = null;

        while ((eLine = dis1.readLine()) != null) {
          fLine = dis2.readLine().trim();
          eLine = eLine.trim();

          String[] tokens;
          if (fTokenizer == null) {
            tokens = fLine.split(" ");
          } else {
            tokens = fTokenizer.processContent(fLine);
          }
          //lastSentLenF = tokens.length;

          for (String token : tokens) {
            if (!fSent.containsKey(token)) { // if this is first time we saw token in this sentence
                System.out.println("F token " + token);
                int f = fVocabSrc.get(token);
                if(f != -1){
                    int[] eS = f2eProbs.get(f).getTranslations(0.0f);
                    for(int transf : eS){
                        String eword = eVocabTgt.get(transf);
                        System.out.println("\t" + eword);
                    }
                }
           }
           fSent.increment(token);

          }
          
          tokens = eTokenizer.processContent(eLine);
          //lastSentLenE = tokens.length;

          for (String token : tokens) {
            if (!eSent.containsKey(token)) {
                System.out.println("E token " + token);
                int e = eVocabSrc.get(token);
                if(e != -1){
                    int[] fS = e2fProbs.get(e).getTranslations(0.0f);
                    for(int transe : fS){
                        String fword = fVocabTgt.get(transe);
                        System.out.println("\t" + fword);
                    }
                }
            }
            eSent.increment(token);
          }
          
        }
        
        } catch (FileNotFoundException e) {
            e.printStackTrace();
          } catch (IOException e) {
            e.printStackTrace();
          }

    }
    
    /*
     * 
     * To run this code:
     etc/run.sh courseproj.clir.CLIRMHUtils data/gt-ferhan/europarl-v6.sample.en data/gt-fern/europarl-v6.sample.de \
       ../Ivory/data/vocab/vocab.en-de.en ../Ivory/data/vocab/vocab.de-en.en ../Ivory/data/vocab/vocab.de-en.de ../Ivory/data/vocab/vocab.en-de.de \
       ../Ivory/data/tokenizer/en-token.bin ../Ivory/data/tokenizer/de-token.bin \
       ../Ivory/data/tokenizer/en.stop ../Ivory/data/tokenizer/de.stop \
       ../Ivory/data/vocab/ttable.en-de ../Ivory/data/vocab/ttable.de-en 
    
    */
    public static void main(String args[]){
        if(args.length != 12){
            System.out.println("Usage: CLIRMHUtils ");
            System.exit(-1);
        }
        String eReadFile = args[0];
        String fReadFile = args[1];
        String eLang = "en";
        String fLang = "de";
        String eVocabSrcFile = args[2];
        String eVocabTgtFile = args[3];
        String fVocabSrcFile = args[4];
        String fVocabTgtFile = args[5];
        String eTokenizerFile = args[6];
        String fTokenizerFile = args[7];
        String eStopwordsFile = args[8];
        String fStopwordsFile = args[9];
        String probTablee2fFile = args[10];
        String probTablef2eFile = args[11];
        FileSystem localFs = null;

        try {
        localFs = FileSystem.getLocal(new Configuration());
        Vocab eVocabSrc = HadoopAlign.loadVocab(new Path(eVocabSrcFile), localFs);
        Vocab eVocabTgt = HadoopAlign.loadVocab(new Path(eVocabTgtFile), localFs);
        Vocab fVocabSrc = HadoopAlign.loadVocab(new Path(fVocabSrcFile), localFs);
        Vocab fVocabTgt = HadoopAlign.loadVocab(new Path(fVocabTgtFile), localFs);
        
        TTable_monolithic_IFAs f2eProbs = null;
        TTable_monolithic_IFAs e2fProbs = null;
        try{
            f2eProbs = new TTable_monolithic_IFAs(localFs, new Path(probTablef2eFile), true);
        }catch(EOFException e){
            
        }
        try{
            e2fProbs = new TTable_monolithic_IFAs(localFs, new Path(probTablee2fFile), true);
        }catch(EOFException e){
            
        }

            readSentences(eReadFile, fReadFile, eLang, fLang,
                    eVocabSrc, eVocabTgt, fVocabSrc, fVocabTgt, eTokenizerFile, fTokenizerFile, eStopwordsFile, fStopwordsFile,
                    e2fProbs, f2eProbs);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InstantiationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    
}
