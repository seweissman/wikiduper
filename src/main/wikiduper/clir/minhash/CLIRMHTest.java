package wikiduper.clir.minhash;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import wikiduper.hash.MultiplyShiftHash;

import ivory.core.tokenize.Tokenizer;
import ivory.core.tokenize.TokenizerFactory;
import edu.umd.cloud9.io.map.HMapSIW;
import edu.umd.cloud9.io.pair.PairOfFloatInt;
import edu.umd.cloud9.util.array.ArrayListOfLongs;
import edu.umd.hooka.Vocab;
import edu.umd.hooka.alignment.HadoopAlign;
import edu.umd.hooka.ttables.TTable_monolithic_IFAs;


public class CLIRMHTest {

    HashMap<String,HashSet<int[]>> e2fbuckets = new HashMap<String, HashSet<int[]>>();
    HashMap<String,HashSet<String[]>> e2fbucketsStr = new HashMap<String, HashSet<String[]>>();
    HashMap<String,HashSet<int[]>> f2ebuckets = new HashMap<String, HashSet<int[]>>();
    HashMap<String,Float> jaccardMap = new HashMap<String,Float>();
    HashMap<String,Float> jaccardActualMap = new HashMap<String,Float>();
    
    MultiplyShiftHash sighashfamily;
    MultiplyShiftHash vocabhashfamily;
    int K;
    int N;
    long sigseed;
    long sampleseed;
    int nHash;
    boolean DEBUG = false;
    int LINELIMIT = 300;
    ArrayList<String> eLines;
    ArrayList<String> fLines;
    float[][][] jaccard;
    
    public CLIRMHTest(int nBits, int nHash, long rseedsig, long rseedvocab, int k, int n,
            String eFilePath, String fFilePath, String jaccardFilePath) throws IOException{
       this.K = k;
       this.N = n;
       this.nHash = nHash;
       long[] sigseeds = new long[nHash];
       long[] vocabseeds = new long[nHash];
       
       int ct = 0;
       Random r = new Random(rseedsig);
       while(ct < nHash){
           sigseeds[ct] = r.nextLong();
           ct++;
       }
       
      r = new Random(rseedvocab);
      ct = 0;
      while(ct < nHash){
           vocabseeds[ct] = r.nextLong();
           ct++;
       }
       
       sigseed = r.nextLong();
       sampleseed = r.nextLong();
       sighashfamily = new MultiplyShiftHash(nBits,sigseeds);
       vocabhashfamily = new MultiplyShiftHash(nBits,vocabseeds);
       
       jaccard = JaccardSim.readJaccardMatrix(jaccardFilePath);
       
       
       File eFile = new File(eFilePath);
       File fFile = new File(fFilePath);
       
     eLines = new ArrayList<String>();
     fLines = new ArrayList<String>();
     
     ct = 0;
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
    
    
    private HashSet<String> wordExpand(HashSet<String> ewordSet, Vocab eVocabSrc, Vocab eVocabTgt, Vocab fVocabSrc, Vocab fVocabTgt, 
            TTable_monolithic_IFAs e2fProbs, TTable_monolithic_IFAs f2eProbs, float prob){
        HashSet<String> wordSet = new HashSet<String>();
        for(String eword : ewordSet){
            //System.out.println("eword = " + eword);
            wordSet.add(eword);
            int e = eVocabSrc.get(eword);
            if(e != -1){
                int fS[] = e2fProbs.get(e).getTranslations(prob);
                for(int f : fS){
                    int fStar = fVocabSrc.get(fVocabTgt.get(f));
                    if(fStar != -1){
                        int eS[] = f2eProbs.get(fStar).getTranslations(prob);
                        for(int eEx : eS){
                            String w = eVocabTgt.get(eEx);
                            wordSet.add(w);
                        }
                    }
                }
            }
        }
        return wordSet;
    }
    
    
    public HashMap<String, HashSet<String>> prepareVocabMap(Vocab fVocabSrc, Vocab fVocabTgt, Vocab eVocabSrc, Vocab eVocabTgt,
            TTable_monolithic_IFAs f2eProbs, TTable_monolithic_IFAs e2fProbs, float fProb, int nH){
        HashMap<String, HashSet<String>> vocabMap = new HashMap<String, HashSet<String>>();

        for(int i=0;i<fVocabSrc.size();i++){
            String token = fVocabSrc.get(i);
            if(token == null) continue;
            if(token.equals("NULL")){
                continue;
            }
            HashSet<String> fWordsOld = new HashSet<String>();
            HashSet<String> fWords = null;
            int fSize = 0;
            int fSizeOld = -1;
            fWordsOld.add(token);
            while(fSize != fSizeOld){
                //System.out.println("old size = " + fSizeOld + " new size = " + fSize + " fVocab size = " + fVocabSrc.size());
                fWords = wordExpand(fWordsOld, fVocabSrc, fVocabTgt, eVocabSrc, eVocabTgt, f2eProbs, e2fProbs, fProb);
                fSizeOld = fWordsOld.size();
                fSize = fWords.size();
                fWordsOld = fWords;
                fProb += .05;
            }
            long minhash[] = new long[nH];
            String hashval[] = new String[nH];
            for(int j=0;j<nH;j++){
                minhash[j] = Long.MAX_VALUE;
            }
            long hash[];
            
            for(String w : fWords){
                hash = vocabhashfamily.hash(w);
                for(int j = 0; j < nH ; j++){
                    if(hash[j] < minhash[j]){
                        minhash[j] = hash[j];
                        hashval[j] = w;
                    }
                }
            }
            if(!vocabMap.containsKey(token)){
                vocabMap.put(token, new HashSet<String>());
            }
            for(int j=0;j<nH;j++){
                vocabMap.get(token).add(hashval[j]);
            }
            
        }
        return vocabMap;
    }
    
    public void minhashVocabSentences(String eReadFile, String fReadFile, String eLang, String fLang,
            Vocab eVocabSrc, Vocab eVocabTgt, Vocab fVocabSrc, Vocab fVocabTgt, String eToken, String fToken, String eStopwordsFile, String fStopwordsFile,
            TTable_monolithic_IFAs e2fProbs, TTable_monolithic_IFAs f2eProbs, 
            float eProb, float fProb) throws IOException,
            ClassNotFoundException, InstantiationException, IllegalAccessException {
          File eFile = new File(eReadFile);
          File fFile = new File(fReadFile);
          
        Tokenizer eTokenizer = TokenizerFactory.createTokenizer(eLang, eToken, true, eStopwordsFile, eStopwordsFile + ".stemmed", null);
        Tokenizer fTokenizer = TokenizerFactory.createTokenizer(fLang, fToken, true, fStopwordsFile, fStopwordsFile + ".stemmed", null);

        int ct = 0;
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
        long minhash[] = new long[nHash];          
        ArrayListOfLongs outsig;
        
        HashMap<String, HashSet<String>> f2fmap = prepareVocabMap(fVocabSrc,fVocabTgt, eVocabSrc, eVocabTgt,
                f2eProbs, e2fProbs, fProb, 3);
        
        while ((eLine = dis1.readLine()) != null) {
            if(ct % 50 == 0){
                System.out.println("Ct = " + ct);
            }

            fLine = dis2.readLine().trim();
            eLine = eLine.trim();
          System.out.println("fline " + ct + ": " + fLine);

          String[] tokens;
          if (fTokenizer == null) {
            tokens = fLine.split(" ");
          } else {
            tokens = fTokenizer.processContent(fLine);
          }
          
          // Process F sentence;

          ArrayList<HashSet<String>> fVecs = new ArrayList<HashSet<String>>();
          long mul = 1;
          for (String ftoken : tokens) {
              if (!fSent.containsKey(ftoken)) { // if this is first time we saw token in this sentence
                  if(f2fmap.containsKey(ftoken)){
                      HashSet<String> fWords = f2fmap.get(ftoken);
                      //System.out.println("ftoken = " + ftoken + " " + fWords.size());

                      //for(String w : fWords){
                      //  System.out.println("\t" + w);
                      //}
                     fVecs.add(fWords);
                     mul *= fWords.size();
                  }
              }
              fSent.increment(ftoken);
          }
          String []dv;
          HashSet<String[]> docSet;
          if(mul < 1000000){
              dv = new String[fVecs.size()];
              docSet = new HashSet<String[]>();
              getDocVecs(fVecs, dv, 0, docSet);
           
              System.out.println("docset size = " + docSet.size());
          //for(String[] d : docSet){
            //  printDV(d);
          //}
          
              for(String[] d : docSet){
                  for(int i=0;i<nHash;i++){
                      minhash[i] = Long.MAX_VALUE;
                  }
                  for(String w : d){
                      long hash[] = sighashfamily.hash(w);
                      for(int j=0;j<nHash;j++){
                          if(hash[j] < minhash[j]){
                              minhash[j] = hash[j];
                         }
                      }
                  }
          
                  Random r = new Random(sigseed);
                  for(int j=0; j<N; j++){
                      outsig = new ArrayListOfLongs();
                      for(int i=0; i<K; i++){
                          outsig.add(i, 0);
                      }
                      for(int i=0; i<K; i++){
                          int x = r.nextInt(nHash);
                          outsig.set(i, minhash[x]);
                      }

                  //System.out.println(outsig);
                      if(!e2fbuckets.containsKey(outsig.toString()))
                          e2fbuckets.put(outsig.toString(), new HashSet<int[]>());
                      int[] id = new int[3];
                      id[0] = 0;
                      id[1] = ct;
                      id[2] = 0;
                      e2fbuckets.get(outsig.toString()).add(id);
                  }
              }


          }
          
          
          // Process E Sentence
          System.out.println("eline: " + eLine);
          tokens = eTokenizer.processContent(eLine);
          fVecs = new ArrayList<HashSet<String>>();          
          mul = 1;
          for (String etoken : tokens) {
              if (!eSent.containsKey(etoken)) { // if this is first time we saw token in this sentence
                  int e = eVocabSrc.get(etoken);    
                  HashSet<String> fWords = new HashSet<String>();
                  if(e != -1){
                      int[] fS = e2fProbs.get(e).getTranslations(eProb);
                      for(int f : fS){
                          String ftoken = fVocabTgt.get(f);
                          if(ftoken != null){
                              HashSet<String> fPart = f2fmap.get(ftoken);
                              if(fPart != null)
                                  fWords.addAll(fPart);
                          }
                      }
                      
                      if(fWords.size() > 0){
                          fVecs.add(fWords);
                          mul *= fWords.size();
                      }
                  }
                  //System.out.println("etoken " + etoken);
                  //System.out.println("Words size " + fWords.size());                  
                  
              }
              eSent.increment(etoken);

          }
          System.out.println("mul = " + mul);
          if(mul < 1000000){
              dv = new String[fVecs.size()];
              docSet = new HashSet<String[]>();
              getDocVecs(fVecs, dv, 0, docSet);
              System.out.println("docset size = " + docSet.size());
              for(String[] d : docSet){
                  for(int i=0;i<nHash;i++){
                      minhash[i] = Long.MAX_VALUE;
                  }
                  for(String w : d){
                      long hash[] = sighashfamily.hash(w);
                      for(int j=0;j<nHash;j++){
                          if(hash[j] < minhash[j]){
                              minhash[j] = hash[j];
                          }
                      }
                  }
          
                  Random r = new Random(sigseed);
                  for(int j=0; j<N; j++){
                      outsig = new ArrayListOfLongs();
                      for(int i=0; i<K; i++){
                          outsig.add(i, 0);
                      }
                      for(int i=0; i<K; i++){
                          int x = r.nextInt(nHash);
                          outsig.set(i, minhash[x]);
                      }

                      //System.out.println(outsig);
                      if(!e2fbuckets.containsKey(outsig.toString()))
                          e2fbuckets.put(outsig.toString(), new HashSet<int[]>());
                      int[] id = new int[3];
                      id[0] = 1;
                      id[1] = ct;
                      id[2] = 0;
                      e2fbuckets.get(outsig.toString()).add(id);
                      }
              }

          }


          fSent = new HMapSIW();
          eSent = new HMapSIW();
          ct++;
        }
        
        } catch (FileNotFoundException e) {
            e.printStackTrace();
          } catch (IOException e) {
            e.printStackTrace();
          }

    }
    
    
    public void minhashFirstSentences(String eLang, String fLang,
            Vocab eVocabSrc, Vocab eVocabTgt, Vocab fVocabSrc, Vocab fVocabTgt, String eToken, String fToken, String eStopwordsFile, String fStopwordsFile,
            TTable_monolithic_IFAs e2fProbs, TTable_monolithic_IFAs f2eProbs, 
            int M) throws IOException,
            ClassNotFoundException, InstantiationException, IllegalAccessException {
        e2fbuckets.clear();
        Tokenizer eTokenizer = TokenizerFactory.createTokenizer(eLang, eToken, true, eStopwordsFile, eStopwordsFile + ".stemmed", null);
        Tokenizer fTokenizer = TokenizerFactory.createTokenizer(fLang, fToken, true, fStopwordsFile, fStopwordsFile + ".stemmed", null);

        int ct = 0;

        HMapSIW fSent = new HMapSIW();
        HMapSIW eSent = new HMapSIW();
        String eLine = null, fLine = null;
        long minhash[] = new long[nHash];          
        ArrayListOfLongs outsig;
        HashSet<String> docset = new HashSet<String>();
        String outval[] = new String[K];
        String hashval[] = new String[nHash];
        for(int line = 0; line < eLines.size(); line++){
            eLine = eLines.get(line);
            fLine = fLines.get(line);
            if(ct % 50 == 0){
                System.out.println("Ct = " + ct);
            }

            String[] tokens;
            if (fTokenizer == null) {
                tokens = fLine.split(" ");
            } else {
                tokens = fTokenizer.processContent(fLine);
            }
          
            if(DEBUG || line == 93 || line == 106) System.out.println("fLine: " + fLine);
          // Process F sentence;
            for(int i=0;i<nHash;i++){
                minhash[i] = Long.MAX_VALUE;
            }

            for (String ftoken : tokens) {
                docset.add(ftoken);
                if (!fSent.containsKey(ftoken)) { // if this is first time we saw token in this sentence
                    long hash[] = sighashfamily.hash(ftoken);
                    for(int j=0;j<nHash;j++){
                        if(hash[j] < minhash[j]){
                            minhash[j] = hash[j];
                            hashval[j] = ftoken;
                        }
                    }
                }

              fSent.increment(ftoken);
            }
            
            Random r = new Random(sigseed);
            for(int j=0; j<N; j++){
                outsig = new ArrayListOfLongs();
                for(int i=0; i<K; i++){
                    outsig.add(i, 0);
                }
                for(int i=0; i<K; i++){
                    int x = r.nextInt(nHash);
                    outsig.set(i, minhash[x]);
                    outval[i] = hashval[x];
                }
                if(DEBUG) System.out.println("j=" + j);
                if(DEBUG) System.out.println(outsig);
                if(DEBUG) printDV(outval);
                
                //System.out.println(outsig);
                if(!e2fbuckets.containsKey(outsig.toString()))
                    e2fbuckets.put(outsig.toString(), new HashSet<int[]>());
                int[] id = new int[2];
                id[0] = 1;
                id[1] = ct;
                e2fbuckets.get(outsig.toString()).add(id);
                
            }
          
            // Process E Sentence
            for(int i=0;i<nHash;i++){
                minhash[i] = Long.MAX_VALUE;
            }

            if(DEBUG) System.out.println(eLine);
            tokens = eTokenizer.processContent(eLine);
            for (String etoken : tokens) {
                if (!eSent.containsKey(etoken)) { // if this is first time we saw token in this sentence
                    long hash[] = sighashfamily.hash(etoken);
                    for(int j=0;j<nHash;j++){
                        if(hash[j] < minhash[j]){
                            minhash[j] = hash[j];
                            hashval[j] = etoken;
                        }
                    }
                }
                eSent.increment(etoken);
            }
            
            if(DEBUG) printDV(hashval);
            Random rSample = new Random(sampleseed);
            r = new Random(sigseed);

            int goodsigct = 0;
            for(int j=0; j<N; j++){
                outsig = new ArrayListOfLongs();
                for(int i=0; i<K; i++){
                    outsig.add(i, 0);
                }
                for(int l=0;l<M;l++){
                    boolean badsig = false;                    
                    for(int i=0;i<K;i++){
                        int x = r.nextInt(nHash);
                        int e = eVocabSrc.get(hashval[x]);
                        if(e == -1){
                            badsig = true;
                        }else{
                            List<PairOfFloatInt> fSProbs = e2fProbs.get(e).getTranslationsWithProbsAsList(0.0f);
                            float psum = 0;
                            float p = rSample.nextFloat();
                            Iterator<PairOfFloatInt> it = fSProbs.iterator();
                            PairOfFloatInt probf = null;
                            int f = -1;
                            while(psum < p && it.hasNext()){
                                probf = it.next();
                                psum += probf.getLeftElement();
                                f = probf.getRightElement();
                            }
                            String fWord = fVocabTgt.get(f);
                            outsig.set(i, sighashfamily.hash(fWord, x));
                            outval[i] = fWord;
                        }
                    }
                    if(!badsig){
                        if(DEBUG) System.out.println("j=" + j + " M=" + l);
                        if(DEBUG) System.out.println(outsig);
                        if(DEBUG) printDV(outval);

                        goodsigct++;
                        if(!e2fbuckets.containsKey(outsig.toString()))
                            e2fbuckets.put(outsig.toString(), new HashSet<int[]>());
                        int[] id = new int[2];
                        id[0] = 0;
                        id[1] = ct;
                        e2fbuckets.get(outsig.toString()).add(id);
                        
                    }
                    
                }
            }
            fSent = new HMapSIW();
            eSent = new HMapSIW();
            ct++;
        }
                

    }
    

    
    String sampleTranslateDistribution(List<PairOfFloatInt> fSProbs, float p, Vocab fVocab){
        Iterator<PairOfFloatInt> it = fSProbs.iterator();
        PairOfFloatInt probf = null;
        int f = -1;
        float psum = 0;
        while(psum <= p && it.hasNext()){
            probf = it.next();
            psum += probf.getLeftElement();
            f = probf.getRightElement();
        }
        String fWord = fVocab.get(f);
        return fWord;
    }

    public void minhashNotFirstSentences(String eLang, String fLang,
            Vocab eVocabSrc, Vocab eVocabTgt, Vocab fVocabSrc, Vocab fVocabTgt, String eToken, String fToken, String eStopwordsFile, String fStopwordsFile,
            TTable_monolithic_IFAs e2fProbs, TTable_monolithic_IFAs f2eProbs, int M) throws IOException,
            ClassNotFoundException, InstantiationException, IllegalAccessException {
        
      Tokenizer eTokenizer = TokenizerFactory.createTokenizer(eLang, eToken, true, eStopwordsFile, eStopwordsFile + ".stemmed", null);
      Tokenizer fTokenizer = TokenizerFactory.createTokenizer(fLang, fToken, true, fStopwordsFile, fStopwordsFile + ".stemmed", null);

      int ct = 0;

      HMapSIW fSent = new HMapSIW();
      HMapSIW eSent = new HMapSIW();
      String eLine = null, fLine = null;
      long minhash[] = new long[nHash];          
      ArrayListOfLongs outsig;
      String hashval[] = new String[nHash];
      HashSet<String> docset = new HashSet<String>();
      for(int line = 0; line < eLines.size(); line++){
          eLine = eLines.get(line);
          fLine = fLines.get(line);
          if(DEBUG || line == 93 || line == 98 ||  line == 106) System.out.println("fLine: " + fLine);
          if(DEBUG || line == 93 ||  line == 98 || line == 106) System.out.println("eLine: " + eLine);
          if(DEBUG || line == 93 ||  line == 98 || line == 106) System.out.println("line:" + line);
          
          if(ct % 50 == 0){
              System.out.println("Ct = " + ct);
          }

         String[] tokens;
         if (fTokenizer == null) {
             tokens = fLine.split(" ");
         } else {
             tokens = fTokenizer.processContent(fLine);
         }
          
         // Process F sentence;
         for(int i=0;i<nHash;i++){
             minhash[i] = Long.MAX_VALUE;
         }
         int tokenct = 0;
         docset.clear();
         for (String ftoken : tokens) {
             docset.add(ftoken);
             if (!fSent.containsKey(ftoken)) { // if this is first time we saw token in this sentence
                 tokenct++;
                 long hash[] = sighashfamily.hash(ftoken);
                 for(int j=0;j<nHash;j++){
                     if(hash[j] < minhash[j]){
                         minhash[j] = hash[j];
                         hashval[j] = ftoken;
                     }
                 }
             }

             fSent.increment(ftoken);
         }
         
         String[] docvec = new String[docset.size() + 2];
         int y = 0;
         for(String w : docset){
             docvec[y] = w;
             y++;
         }
         docvec[docset.size()] = "f";
         docvec[docset.size()+1] = Integer.toString(line);
         if(DEBUG || line == 93 ||  line == 98 || line == 106) System.out.println("ntokens = " + tokenct);
         if(DEBUG || line == 93 || line == 98 ||  line == 106) printDV(hashval);

         Random r = new Random(sigseed);
         for(int j=0; j<N; j++){
             if(DEBUG || line == 93 || line == 98 ||  line == 106) System.out.println("j = " + j);
             outsig = new ArrayListOfLongs();
             for(int i=0; i<K; i++){
                 outsig.add(i, 0);
             }
             for(int i=0; i<K; i++){
                 int x = r.nextInt(nHash);
                 outsig.set(i, minhash[x]);
                 if(DEBUG || line == 93 || line == 98 ||  line == 106) if(i != 0) System.out.print(",");
                 if(DEBUG || line == 93 || line == 98 ||  line == 106) System.out.print(x);
                }
             if(DEBUG || line == 93 || line == 98 ||  line == 106) System.out.println();
             if(DEBUG || line == 93 || line == 98 ||  line == 106) System.out.println(outsig);
             
             if(!e2fbucketsStr.containsKey(outsig.toString()))
                 e2fbucketsStr.put(outsig.toString(), new HashSet<String[]>());
             e2fbucketsStr.get(outsig.toString()).add(docvec);
             
             if(!e2fbuckets.containsKey(outsig.toString()))
                 e2fbuckets.put(outsig.toString(), new HashSet<int[]>());
             int[] id = new int[3];
             id[0] = 0;
             id[1] = ct;
             id[2] = 0;
             e2fbuckets.get(outsig.toString()).add(id);
             
         }
          
            // Process E Sentence

            if(DEBUG || line == 93 ||  line == 98 || line == 106) System.out.println("eline: " + eLine);
            tokens = eTokenizer.processContent(eLine);
            Random rSample = new Random(sampleseed);
            for(int l=0;l<M;l++){
                for(int i=0;i<nHash;i++){
                    minhash[i] = Long.MAX_VALUE;
                }
                tokenct = 0;
                docset.clear();
                for (String etoken : tokens) {
                    if (!eSent.containsKey(etoken)) { // if this is first time we saw token in this sentence
                        int e = eVocabSrc.get(etoken);
                        if(e != -1){
                            tokenct++;
                            List<PairOfFloatInt> fSProbs = e2fProbs.get(e).getTranslationsWithProbsAsList(0.0f);
                            float p = rSample.nextFloat();
                            String fWord = sampleTranslateDistribution(fSProbs, p, fVocabTgt);
                            docset.add(fWord);
                            long hash[] = sighashfamily.hash(fWord);                            
                            for(int j=0;j<nHash;j++){
                                if(hash[j] < minhash[j]){
                                    minhash[j] = hash[j];
                                    hashval[j] = fWord;
                                }
                            }

                        }
                    }
                    eSent.increment(etoken);
                }
                docvec = new String[docset.size() + 2];
                y = 0;
                for(String w : docset){
                    docvec[y] = w;
                    y++;
                }
                docvec[docset.size()] = "e";
                docvec[docset.size()+1] = Integer.toString(line);
                if(DEBUG || line == 93 || line == 98 ||  line == 106) System.out.println("ntokens = " + tokenct);
                if(DEBUG || line == 93 || line == 98 ||  line == 106) printDV(hashval);
                r = new Random(sigseed);
                for(int j=0; j<N; j++){
                    outsig = new ArrayListOfLongs();
                    for(int i=0; i<K; i++){
                        outsig.add(i, 0);
                    }
                    for(int i=0;i<K;i++){
                        int x = r.nextInt(nHash);
                        outsig.set(i, minhash[x]);
                        if(DEBUG || line == 93 || line == 98 || line == 106) if(i != 0) System.out.print(",");
                        if(DEBUG || line == 93 || line == 98 || line == 106) System.out.print(x);
                    }
                    if(DEBUG || line == 93 || line == 98 || line == 106) System.out.println();
                    if(DEBUG || line == 93 || line == 98 || line == 106) System.out.println(outsig);

                    if(!e2fbucketsStr.containsKey(outsig.toString()))
                        e2fbucketsStr.put(outsig.toString(), new HashSet<String[]>());
                    e2fbucketsStr.get(outsig.toString()).add(docvec);
                    
                    if(!e2fbuckets.containsKey(outsig.toString()))
                        e2fbuckets.put(outsig.toString(), new HashSet<int[]>());
                    int[] id = new int[3];
                    id[0] = 1;
                    id[1] = ct;
                    id[2] = l;
                    e2fbuckets.get(outsig.toString()).add(id);
                    
                    
                }
                eSent = new HMapSIW();
            }
            fSent = new HMapSIW();
            ct++;
        }
                
    }
    

    private void printDV(String[] v){
        System.out.print("[");
        for(int i=0;i<v.length;i++){
            if(i != 0)
                System.out.print(",");
            System.out.print(v[i]);
        }
        System.out.println("]");
        
    }
    private void printHash(long[] hash) {

        System.out.print("[");
        for(int i=0;i<hash.length;i++){
            if(i != 0)
                System.out.print(",");
            System.out.print(hash[i]);
        }
        System.out.println("]");
    }


    private void readSentences(String eReadFile, String fReadFile, String eLang, String fLang,
            Vocab eVocabSrc, Vocab eVocabTgt, Vocab fVocabSrc, Vocab fVocabTgt, String eToken, String fToken, 
            String eStopwordsFile, String fStopwordsFile,
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
          
          for (String token : tokens) {
            if (!fSent.containsKey(token)) { // if this is first time we saw token in this sentence

                System.out.println("F token " + token);
                int f = fVocabSrc.get(token);
                if(f != -1){
                    int[] eS = f2eProbs.get(f).getTranslations(0.05f);
                    for(int transf : eS){
                        String eword = eVocabTgt.get(transf);
                        System.out.println("\t" + eword);
                    }
                }
           }
           fSent.increment(token);

          }
          
          
          
          
          tokens = eTokenizer.processContent(eLine);

          for (String token : tokens) {
            if (!eSent.containsKey(token)) { // if this is first time we saw token in this sentence
                System.out.println("E token " + token);
                int e = eVocabSrc.get(token);
                if(e != -1){
                    int[] fS = e2fProbs.get(e).getTranslations(0.05f);
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
    
    private void getDocVecs(ArrayList<HashSet<String>> fVecs, String[] dv, int i, HashSet<String[]> docSet) {
        if(i == fVecs.size()){
            String[] outsig = new String[i];
            for(int j=0;j<i;j++){
                outsig[j] = dv[j];
            }

            docSet.add(outsig);
        }else{
            for(String w : fVecs.get(i)){
                dv[i] = w;
                getDocVecs(fVecs, dv, i+1, docSet);
            }
        }

    }


    
    public void getSignatures(ArrayList<TreeSet<Long>> transSig, ArrayListOfLongs sig, HashMap<ArrayListOfLongs,HashSet<String>> bucket, String sentence, int i){
        //System.out.println("i=" + i + " size = " + transSig.size());
        if(i == transSig.size()){
            ArrayListOfLongs outsig = new ArrayListOfLongs();
            for(long l : sig){
                outsig.add(l);
            }
            System.out.println("outsig : " + outsig);
            if(!bucket.containsKey(outsig))
                bucket.put(outsig, new HashSet<String>());
            bucket.get(outsig).add(sentence);
        }else{
            for(long l : transSig.get(i)){
                sig.set(i,l);
                getSignatures(transSig, sig, bucket, sentence, i+1);
            }
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
       
    

    public static void runMinhashFirstTest(CLIRMHTest clirtest, String eLang, String fLang,
            Vocab eVocabSrc, Vocab eVocabTgt, Vocab fVocabSrc, Vocab fVocabTgt, String eToken, String fToken, String eStopwordsFile, String fStopwordsFile,
            TTable_monolithic_IFAs e2fProbs, TTable_monolithic_IFAs f2eProbs, int M) throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException{

        String jaccMatchOutFile = "mhfirst-matches.out";
        String jaccNoMatchOutFile = "mhfirst-nomatches.out";
        
        int nSamples = 100;
        clirtest.minhashFirstSentences(eLang, fLang, eVocabSrc, eVocabTgt, fVocabSrc, fVocabTgt, 
                eToken, fToken, eStopwordsFile, fStopwordsFile,
                e2fProbs, f2eProbs, nSamples);
        
        TreeSet<String> matchSet = new TreeSet<String>();
        int goodmatch = 0;
        int badmatch = 0;

        boolean matches[][] = new boolean[clirtest.eLines.size()][clirtest.fLines.size()];
        for(int i=0; i< matches.length; i++){
            for(int j=0; j<matches[i].length; j++){
                matches[i][j] = false;
            }
        }
        
        File matchFile = new File(jaccMatchOutFile);
        File nomatchFile = new File(jaccNoMatchOutFile);
        
        float jaccardActual[][] = clirtest.jaccardActual(eLang, eToken, eStopwordsFile);
        
      FileOutputStream fos1 = null, fos2 = null;
      BufferedWriter dos1 = null, dos2 = null;
          
          fos1 = new FileOutputStream(matchFile);
          fos2 = new FileOutputStream(nomatchFile);
          dos1 = new BufferedWriter(new OutputStreamWriter(fos1));
          dos2 = new BufferedWriter(new OutputStreamWriter(fos2));
          System.out.println("N buckets = " +  clirtest.e2fbuckets.keySet().size());
          for(String sig : clirtest.e2fbuckets.keySet()){
              if(clirtest.e2fbuckets.get(sig).size() > 1){
                  Iterator<int[]> it1 = clirtest.e2fbuckets.get(sig).iterator();
                  Iterator<int[]> it2 = clirtest.e2fbuckets.get(sig).iterator();
                  while(it1.hasNext()){
                      int id1[] = it1.next();
                      String l1 = Integer.toString(id1[0]) + "-" + Integer.toString(id1[1]);

                      while(it2.hasNext()){
                          int id2[] = it2.next();
                          String l2 = Integer.toString(id2[0]) + "-" + Integer.toString(id2[1]);
                          if(id1[0] != id2[0]){ // different language matches
                              if(id1[0] == 1){
                                  String match = l2 + "," + l1;
                                  
                                  if(!matchSet.contains(match)){
                                      System.out.println("match1 = " + match);
                                      matchSet.add(match);
                                  }
                                  matches[id2[1]][id1[1]] = true;
                              }else{
                                  String match = l1 + "," + l2;
                                  
                                  if(!matchSet.contains(match)){
                                      System.out.println("match0 = " + match);
                                      matchSet.add(match);
                                  }
                                  matches[id1[1]][id2[1]] = true;
                              }
                          }
                      }
                  }
              }
          }
          
          for(int i=0; i< matches.length; i++){
              for(int j=0; j<matches[i].length; j++){
                  float jsim = jaccardActual[i][j];
                  if(!matches[i][j]){
                      dos2.write(Float.toString(jsim));
                      dos2.write("\n");
                  }else{
                      if(i==j) goodmatch++;
                      dos1.write(Float.toString(jsim));
                      dos1.write("\n");
                  }
              }
          }
          

          dos1.close();
          dos2.close();
          fos1.close();
          fos2.close();
          
          System.out.println("good matches " + goodmatch);
          
    }
        */
    public static void runMinhashNotFirstTest(CLIRMHTest clirtest, String eLang, String fLang,
            Vocab eVocabSrc, Vocab eVocabTgt, Vocab fVocabSrc, Vocab fVocabTgt, String eToken, String fToken, String eStopwordsFile, String fStopwordsFile,
            TTable_monolithic_IFAs e2fProbs, TTable_monolithic_IFAs f2eProbs, int M) throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException{

        String jaccMatchOutFile = "matches.out";
        String jaccNoMatchOutFile = "nomatches.out";
        
        //String jaccActualMatchOutFile = "matches-actual.out";
        //String jaccActualNoMatchOutFile = "nomatches-actual.out";
        
        int nSamples = 100;
        clirtest.minhashNotFirstSentences(eLang, fLang, eVocabSrc, eVocabTgt, fVocabSrc, fVocabTgt, 
                eToken, fToken, eStopwordsFile, fStopwordsFile,
                e2fProbs, f2eProbs, nSamples);
        
        //float jaccardActual[][] = clirtest.jaccardActual(eLang, eToken, eStopwordsFile);
        
        TreeSet<String> matchSet = new TreeSet<String>();
        int goodmatch = 0;
        int badmatch = 0;
        int zeroct = 0;
        int onect = 0;

        boolean matches[][] = new boolean[clirtest.eLines.size()][clirtest.eLines.size()];
        for(int i=0; i< matches.length; i++){
            for(int j=0; j<matches[i].length; j++){
                matches[i][j] = false;
            }
        }
        
        HashMap<Float,Integer> avgJaccardNoMatchFreqs = new HashMap<Float, Integer>();
        
        File matchFile = new File(jaccMatchOutFile);
        File nomatchFile = new File(jaccNoMatchOutFile);
        
      FileOutputStream fos1 = null, fos2 = null;
      BufferedWriter dos1 = null, dos2 = null;
          
          fos1 = new FileOutputStream(matchFile);
          fos2 = new FileOutputStream(nomatchFile);
          dos1 = new BufferedWriter(new OutputStreamWriter(fos1));
          dos2 = new BufferedWriter(new OutputStreamWriter(fos2));
          HashSet<String> eSet = new HashSet<String>();
          HashSet<String> fSet = new HashSet<String>();
          System.out.println("N buckets = " +  clirtest.e2fbucketsStr.keySet().size());
          for(String sig : clirtest.e2fbucketsStr.keySet()){
              if(clirtest.e2fbucketsStr.get(sig).size() > 1){
                  Iterator<String[]> it1 = clirtest.e2fbucketsStr.get(sig).iterator();
                  Iterator<String[]> it2 = clirtest.e2fbucketsStr.get(sig).iterator();
                  while(it1.hasNext()){
                      String docvec1[] = it1.next();
                      int len1 = docvec1.length;
                      String l1 = docvec1[len1 - 2] + docvec1[len1 - 1];
                      int index1 = Integer.parseInt(docvec1[len1 - 1]);
                      
                      while(it2.hasNext()){
                          String docvec2[] = it2.next();
                          int len2 = docvec2.length;
                          String l2 = docvec2[len2-2] + docvec2[len2-1];
                          int index2 = Integer.parseInt(docvec2[len2-1]);
                          if(docvec1[len1-2].compareTo(docvec2[len2-2]) != 0){
                              float jsim;
                              eSet.clear();
                              fSet.clear();
                              for(int i=0;i<docvec1.length-2;i++){
                                  eSet.add(docvec1[i]);
                              }
                              for(int i=0;i<docvec2.length-2;i++){
                                  fSet.add(docvec2[i]);
                              }

                              //jsim = JaccardSim.averageJaccard(clirtest.jaccard, index1, index2);

                              String lang = docvec1[len1-2];
                              if(lang.compareTo("f") == 0){
                                  String match = l2 + "," + l1;
                                  
                                  if(!matchSet.contains(match)){
                                      jsim = JaccardSim.maxJaccard(clirtest.jaccard, index2, index1);
                                      if(jsim == 0.0f){
                                          System.out.println("0 similarity");
                                       }

                                      System.out.print("evec ");
                                      clirtest.printDV(docvec1);
                                      System.out.print("fvec ");
                                      clirtest.printDV(docvec2);
                                      System.out.println("match1 = " + match + " Jaccard " + jsim); 
                                      //System.out.println("Actual jaccard indexes " + index2 + " " + index1 + " " + jaccardActual[index2][index1]);
                                      //System.out.println(clirtest.eLines.get(index2));
                                      //System.out.println(clirtest.fLines.get(index1));
                                      //System.out.println(clirtest.fLines.get(index2));
                                      dos1.write(Float.toString(jsim));
                                      dos1.write("\n");
                                      matchSet.add(match);
                                  }
                                  matches[index2][index1] = true;
                              }else{
                                  String match = l1 + "," + l2;
                                  
                                  if(!matchSet.contains(match)){
                                      jsim = JaccardSim.maxJaccard(clirtest.jaccard, index1, index2);
                                      if(jsim == 0.0f){
                                          System.out.println("0 similarity");
                                      }
                                      
                                      System.out.print("evec ");
                                      clirtest.printDV(docvec1);
                                      System.out.print("fvec ");
                                      clirtest.printDV(docvec2);
                                      System.out.println("match0 = " + match + " Jaccard " + jsim);
                                      //System.out.println("Actual jaccard indexes " + index1 + " " + index2 + " " + jaccardActual[index1][index2]);
                                      //System.out.println(clirtest.eLines.get(index2));
                                      //System.out.println(clirtest.fLines.get(index1));
                                      //System.out.println(clirtest.fLines.get(index2));
                                      dos1.write(Float.toString(jsim));
                                      dos1.write("\n");
                                      matchSet.add(match);
                                  }
                                  matches[index1][index2] = true;
                              }
                          }
                      }
                  }
              }else{
                  System.out.print("Singleton, sig= "+sig);
                  Iterator<String[]> it1 = clirtest.e2fbucketsStr.get(sig).iterator();
                  clirtest.printDV(it1.next());
              }
          }
          
          
          
          for(int i=0; i< matches.length; i++){
              for(int j=0; j<matches[i].length; j++){
                  if(!matches[i][j]){
                      //float avgjsim = JaccardSim.averageJaccard(clirtest.jaccard,i,j);
                      float avgjsim = JaccardSim.maxJaccard(clirtest.jaccard,i,j);
                      if(avgjsim >= .95f){
                          System.out.println("1.0 non match: " + "i=" + i + " j=" + j + " jsim=" + avgjsim);
                          System.out.println(clirtest.eLines.get(i));
                          System.out.println(clirtest.fLines.get(j));
                      }
                      if(!avgJaccardNoMatchFreqs.containsKey(avgjsim)){
                          avgJaccardNoMatchFreqs.put(avgjsim, 0);
                      }
                      avgJaccardNoMatchFreqs.put(avgjsim, avgJaccardNoMatchFreqs.get(avgjsim)+1);
                  }else{
                      if(i==j) goodmatch++;
                  }
              }
          }
          
          //System.out.println("Average Jaccard Match Freqs");
            /*
          for(float jsim : avgJaccardMatchFreqs.keySet()){
              int freq = avgJaccardMatchFreqs.get(jsim);
              for(int i=0;i<freq;i++){
                  dos1.write(Float.toString(jsim));
                  dos1.write("\n");
              }
          }
          */
          //System.out.println("Average Jaccard No Match Freqs");
          for(float jsim : avgJaccardNoMatchFreqs.keySet()){
              int freq = avgJaccardNoMatchFreqs.get(jsim);
              for(int i=0;i<freq;i++){
                  dos2.write(Float.toString(jsim));
                  dos2.write("\n");
              }
          }
          dos1.close();
          dos2.close();
          fos1.close();
          fos2.close();
          System.out.println("good matches " + goodmatch);
        
          /*
          
          matchFile = new File(jaccActualMatchOutFile);
          nomatchFile = new File(jaccActualNoMatchOutFile);
            
            fos1 = new FileOutputStream(matchFile);
            fos2 = new FileOutputStream(nomatchFile);
            dos1 = new BufferedWriter(new OutputStreamWriter(fos1));
            dos2 = new BufferedWriter(new OutputStreamWriter(fos2));

            for(int i=0; i< matches.length; i++){
                for(int j=0; j<matches[i].length; j++){
                    float jsim = jaccardActual[i][j];
                    
                    if(!matches[i][j]){
                        dos2.write(Float.toString(jsim));
                        dos2.write("\n");
                    }else{
                        if(i==j) goodmatch++;
                        dos1.write(Float.toString(jsim));
                        dos1.write("\n");
                    }
                }
            }
            
            dos1.close();
            dos2.close();
            fos1.close();
            fos2.close();
*/
    }
    
    
    public static void main(String args[]){
        if(args.length != 13){
            System.out.println("Usage: CLIRMHUtils ");
            System.exit(-1);
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
        String jaccardInFile = args[12];
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
            int K = 10;
            CLIRMHTest clirtest = new CLIRMHTest(30,20,11543969, 887373727,K,20,eReadFile, fReadFile, jaccardInFile);

            int nSamples = 100;
            
            runMinhashNotFirstTest(clirtest, eLang, fLang, eVocabSrc, eVocabTgt, fVocabSrc, fVocabTgt, 
                    eTokenizerFile, fTokenizerFile, eStopwordsFile, fStopwordsFile,
                    e2fProbs, f2eProbs, nSamples);
            
            /*
            runMinhashFirstTest(clirtest, fLang, eLang, fVocabSrc, fVocabTgt, eVocabSrc, eVocabTgt, 
                    fTokenizerFile, eTokenizerFile, fStopwordsFile, eStopwordsFile,
                    f2eProbs, e2fProbs, nSamples);
            */

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
