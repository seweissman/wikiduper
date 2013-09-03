package wikiduper.clir.minhash;

import ivory.core.tokenize.Tokenizer;
import ivory.core.tokenize.TokenizerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.pair.PairOfFloatInt;
import edu.umd.hooka.Vocab;
import edu.umd.hooka.alignment.HadoopAlign;
import edu.umd.hooka.ttables.TTable_monolithic_IFAs;

public class JaccardSim {


    public static boolean DEBUG = false;
    public static int LINELIMIT = 300;
    public static ArrayList<String> eLines = new ArrayList<String>();
    public static ArrayList<String> fLines = new ArrayList<String>();
    
    public static float averageJaccard(float jaccard[][][], int e, int f){
        float avg = 0.0f;
        float[] jacsamp = jaccard[e][f];
        for(int i=0; i< jacsamp.length; i++){
            avg += jacsamp[i];
        }
        return avg/jacsamp.length;
    }

    
    public static float maxJaccard(float jaccard[][][], int e, int f){
        float max = 0.0f;
        float[] jacsamp = jaccard[e][f];
        for(int i=0; i< jacsamp.length; i++){
            //System.out.println("jacsamp " + i + " " + jacsamp[i]);
            if(jacsamp[i] > max) max = jacsamp[i];
        }
        return max;
    }

    
    
    public static void writeJaccardMatrix(String outPath, float jaccard[][][], int nSamp) throws IOException{
        File outFile = new File(outPath);
        //FileWriter fos = new FileWriter(outFile);
        //BufferedWriter dos = new BufferedWriter(fos);
        FileOutputStream fos = new FileOutputStream(outFile);
        DataOutputStream dos = new DataOutputStream(fos);
        dos.writeInt(jaccard.length);
        dos.writeInt(nSamp);
        for(int i=0;i<jaccard.length;i++){
            for(int j=0;j<jaccard[i].length; j++){
                for(int s=0;s<jaccard[i][j].length; s++){
                    dos.writeFloat(jaccard[i][j][s]);
                    //System.out.println("Wrote: " + i + " " + j + " " + s + " " + jaccard[i][j][s]);
                }
            }
        }
        dos.close();
    }
    
    public static float[][][] readJaccardMatrix(String inPath) throws IOException{
        File inFile = new File(inPath);
        FileInputStream fis = new FileInputStream(inFile);
        DataInputStream dis = new DataInputStream(fis);
        int len = 0;
        int nSamp = 0;

        len = dis.readInt();
        nSamp = dis.readInt();
        float[][][] jaccard = new float[len][len][nSamp];
        for(int i=0;i<jaccard.length;i++){
            System.out.println("Line:" + i);
            for(int j=0;j<jaccard[i].length; j++){
                for(int s=0;s<jaccard[i][j].length; s++){
                    jaccard[i][j][s] = dis.readFloat();
                    //System.out.println("Read: " + i + " " + j + " " + s + " " + jaccard[i][j][s]);
                }
            }
        }       
        dis.close();
        return jaccard;
    }
    
    public static void readLines(String eFilePath, String fFilePath) throws IOException{
        
        
        File eFile = new File(eFilePath);
        File fFile = new File(fFilePath);
        eLines.clear();
        fLines.clear();
      
      int ct = 0;
      FileInputStream fis1 = null, fis2 = null;
      BufferedReader dis1 = null, dis2 = null;
          
          fis1 = new FileInputStream(eFile);
          fis2 = new FileInputStream(fFile);
          dis1 = new BufferedReader(new InputStreamReader(fis1, "UTF-8"));
          dis2 = new BufferedReader(new InputStreamReader(fis2, "UTF-8"));
          String eLine = null, fLine = null;
          while ((eLine = dis1.readLine()) != null && ct < LINELIMIT) {
            fLine = dis2.readLine().trim();
            eLine = eLine.trim();
            eLines.add(eLine);
            fLines.add(fLine);
            ct++;
          }
          dis1.close();
          dis2.close();
          fis1.close();
          fis2.close();
     }
     
    public static float jaccardSim(HashSet<String> eSet, HashSet<String> fSet){
        
        HashSet<String> efSet = new HashSet<String>();
        efSet.addAll(fSet);
        efSet.addAll(eSet);
        float intersect = 0;
        for(String w : fSet){
            if(eSet.contains(w)){
                intersect++;
            }
        }
        return intersect/efSet.size();
    }
    
    public static float jaccardSim(String[] eList, String[] fList){
        HashSet<String> eSet = new HashSet<String>();
        HashSet<String> fSet = new HashSet<String>();
        HashSet<String> efSet = new HashSet<String>();
        for(String w : eList){
            eSet.add(w);
        }
        for(String w : fList){
            fSet.add(w);
        }
        efSet.addAll(eSet);
        efSet.addAll(fSet);
        float intersect = 0;
        for(String w : fSet){
            if(eSet.contains(w)){
                intersect++;
            }
        }
        return intersect/efSet.size();
    }
    public float[][] jaccardActual(String eLang, String eToken, String eStopWordsFile){
        Tokenizer eTokenizer = TokenizerFactory.createTokenizer(eLang, eToken, true, eStopWordsFile, eStopWordsFile + ".stemmed", null);
        HashSet<String> e1Set = new HashSet<String>();
        HashSet<String> e2Set = new HashSet<String>();
        float jaccard[][] = new float[eLines.size()][eLines.size()];
        String [] e1tokens;
        String [] e2tokens;
        String e1Line;
        String e2Line;
        for(int ii=0; ii< eLines.size(); ii++){
            System.out.println("Line:" + ii);
            e1Line = eLines.get(ii);
            e1tokens = eTokenizer.processContent(e1Line);
            e1Set = new HashSet<String>();
            for(String token : e1tokens){
              e1Set.add(token);  
            }
             for(int jj=0; jj<eLines.size(); jj++){
                 e2Set = new HashSet<String>();
                 e2Line = eLines.get(jj);
                 e2tokens = eTokenizer.processContent(e2Line);
                 for(String token : e2tokens){
                   e2Set.add(token);  
                 }
                 jaccard[ii][jj] = jaccardSim(e1Set, e2Set);
                 //System.out.println(e1Set);
                 //System.out.println(e2Set);
                 //System.out.println("jsim " + jaccard[ii][jj]);
             }
        }
        return jaccard;
        
    }
    
    public static float[][][] jaccardDist(String eLang, String fLang,Vocab eVocabSrc, Vocab eVocabTgt, Vocab fVocabSrc, Vocab fVocabTgt, 
            String eToken, String fToken, String eStopwordsFile, String fStopwordsFile,
            TTable_monolithic_IFAs e2fProbs, TTable_monolithic_IFAs f2eProbs, int M, long sampleseed) throws IOException,
            ClassNotFoundException, InstantiationException, IllegalAccessException {
        
        Tokenizer eTokenizer = TokenizerFactory.createTokenizer(eLang, eToken, true, eStopwordsFile, eStopwordsFile + ".stemmed", null);
      Tokenizer fTokenizer = TokenizerFactory.createTokenizer(fLang, fToken, true, fStopwordsFile, fStopwordsFile + ".stemmed", null);

      HashSet<String> fSet = new HashSet<String>();
      HashSet<String> eSet = new HashSet<String>();
      String eLine = null, fLine = null;
      float jaccard[][][] = new float[eLines.size()][fLines.size()][M];
      String[] tokens;
      for(int ii=0; ii< eLines.size(); ii++){
          System.out.println("Line: " + ii);
           for(int jj=0; jj<fLines.size(); jj++){


              eLine = eLines.get(ii);
              fLine = fLines.get(jj);
              tokens = fTokenizer.processContent(fLine);
              fSet.clear();
          // Process F sentence;
              for (String ftoken : tokens) {
                  fSet.add(ftoken);
              }
                          
          // Process E Sentence

              if(DEBUG) System.out.println("eline: " + eLine);
              tokens = eTokenizer.processContent(eLine);
            
              Random rSample = new Random(sampleseed);
              for(int l=0;l<M;l++){
                  eSet.clear();
                  HashSet<String> eTokens = new HashSet<String>();
                  for (String etoken : tokens) {
                    if (!eTokens.contains(etoken)) { // if this is first time we saw token in this sentence
                        int e = eVocabSrc.get(etoken);
                        if(e != -1){
                            List<PairOfFloatInt> fSProbs = e2fProbs.get(e).getTranslationsWithProbsAsList(0.0f);
                            float psum = 0;
                            float p = rSample.nextFloat();
                            Iterator<PairOfFloatInt> it = fSProbs.iterator();
                            PairOfFloatInt probf = null;
                            int f = -1;
                            while(psum <= p && it.hasNext()){
                                probf = it.next();
                                psum += probf.getLeftElement();
                                f = probf.getRightElement();
                            }
                            String fWord = fVocabTgt.get(f);
                            eSet.add(fWord);
                        }
                    }
                    eTokens.add(etoken);
                }
                  jaccard[ii][jj][l] = jaccardSim(fSet, eSet);
              }
          }
      }
      return jaccard;
                
    }
    
    public static void main(String args[]){
        if(args.length < 13){
            System.out.print("Usage MaxJaccardSim ...");
            return;
        }

        String eReadFile = args[0];
        String fReadFile = args[1];
        String eLang = "de";
        String fLang = "en";
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
        String jaccOutFile = args[12];
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
            }catch(EOFException e){}
            try{
                e2fProbs = new TTable_monolithic_IFAs(localFs, new Path(probTablee2fFile), true);
            }catch(EOFException e){}
            int nSamples = 100;
            
            JaccardSim.readLines(eReadFile, fReadFile);
            float jaccard[][][] = JaccardSim.jaccardDist(eLang, fLang, eVocabSrc, eVocabTgt, fVocabSrc, fVocabTgt, 
                    eTokenizerFile, fTokenizerFile, eStopwordsFile, fStopwordsFile,
                    e2fProbs, f2eProbs, 100, 887373727);
            JaccardSim.writeJaccardMatrix(jaccOutFile, jaccard, nSamples);

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
        long sampleseed = 2992388;
        

    }


    public static float jaccardSim(ArrayListWritable<Text> eSentence,
            ArrayListWritable<Text> fSentence) {
        HashSet<String> eSet = new HashSet<String>();
        HashSet<String> fSet = new HashSet<String>();
        for(Text w : eSentence){
            eSet.add(w.toString());
        }
        for(Text w : fSentence){
            fSet.add(w.toString());
        }
        return jaccardSim(eSet,fSet);

    }
    
}
