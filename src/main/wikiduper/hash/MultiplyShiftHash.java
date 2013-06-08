package wikiduper.hash;

import java.util.Random;

/**
 * Following the "hashing vectors" scheme found here:
 * http://en.wikipedia.org/wiki/Universal_hashing

 * Creates a family of hashes that map a vector of longs
 * to an m-bit value (where m <= 64 bits)
 * 
 * The family H is initialized with a vector of random seeds, one for each
 * hash function. (So |H| = |seeds|.)
 * 
 * Each call to hash(x) returns a vector v here v_i = h_i(x).
 * 
 */
public class MultiplyShiftHash {

  long []seeds;
  int m;
  
  /**
   * Initializes family of |seeds| hash functions according to seeds. Each
   * hash output is of size m bits.
   * 
   * @param m output size of the hash
   * @param seeds
   */
  public MultiplyShiftHash(int m, long seeds[]){
    this.seeds = seeds;
    this.m = m;
  }
  
  /**
   * Hashes a variable length input packed as a vector of ints to a vector
   * of hashes w where w_i is h_i(v).
   * 
   * @param v
   * @return
   */
  public long[] hash(long []v){
    long hashvec[] = new long[seeds.length];
    for(int s=0;s<seeds.length;s++){
      Random r = new Random(seeds[s]);

      long sum = 0;
      for(int i=0;i<v.length; i++){
        long a = r.nextLong();
        if(a%2 == 0){
          // make sure a is odd (better way to do this?)
          a += 1;
        }
        sum += v[i]*a;
      }
      hashvec[s] = sum >>> (64 - m);

    }
    return hashvec;
  }
  
  /**
   * Hashes a variable length string input
   * 
   * @param str
   * @return
   */
  public long[] hash(String str){
    byte b[] = str.getBytes();
    long[] v = new long[8*b.length/Long.SIZE + 1];
    //System.out.println("n bytes = " + b.length);
    //System.out.println("n long = " + v.length);
    for(int i=0;i<b.length;i++){
      v[i/Long.SIZE] |= (b[i] & 0xff) << (8*(i%Long.SIZE));
    }
    
    return hash(v);
  }
  
  public static void hashlongtest(MultiplyShiftHash hashfamily){

    for(int v=0; v<256; v++){
      long vec[] = new long[10];
      for(int i=0;i<10;i++){
        vec[i] = v + i;
      }
      long hashv[] = hashfamily.hash(vec);
      System.out.print(v + ",");
      System.out.print("[");
      for(int i=0;i<hashv.length;i++){
        if(i != 0) System.out.print(", ");
        System.out.print(hashv[i]);
      }
      System.out.println("]");
    }

    
  }
  
  
  public static void hashstringtest(MultiplyShiftHash hashfamily){
    String stest = "Determined to expose the person who drugged and assaulted her at a party the year before, Veronica questions her friends and many rivals about the events that occurred that night at Shelly Pomeroy's (MELISSA HOOVER) house. Meanwhile, when one of Veronica and Wallace's stunts affects Alicia's (recurring guest star ERICA GIMPEL) job at Kane Software, she confronts Keith about his daughter's behavior. Later, in an attempt to get closer to his son, Aaron (recurring guest star HARRY HAMLIN) hosts a birthday party for Logan that surprises everyone.";
    String stestsplit[] = stest.split(" "); 
    System.out.println("Split length " + stestsplit.length);
    for(int i=0; i<stestsplit.length; i++){
      String s = stestsplit[i];
            
      long hashv[] = hashfamily.hash(s);
      //System.out.print(s + ",");
      //System.out.print("[");
      for(int j=0;j<hashv.length;j++){
        //if(j != 0) System.out.print(", ");
        System.out.println(hashv[j]);
      }
      //System.out.println("]");
    }
  }
  
  public static void main(String args[]){
    long seeds[] = {2343,2299,6632,9862};
    MultiplyShiftHash hashfamily = new MultiplyShiftHash(10,seeds);
    
    //System.out.println("Long test:");
    //hashlongtest(hashfamily);
    System.out.println("String test:");
    hashstringtest(hashfamily);
    
  }
}
