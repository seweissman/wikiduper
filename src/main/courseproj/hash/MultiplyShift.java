package courseproj.hash;

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
public class MultiplyShift {

  long []seeds;
  int m;
  
  /**
   * Initializes family of |seeds| hash functions according to seeds. Each
   * hash output is of size m bits.
   * 
   * @param m output size of the hash
   * @param seeds
   */
  public MultiplyShift(int m, long seeds[]){
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
  public long[] hash(int []v){
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
  
  public static void main(String args[]){
    long seeds[] = {2343,2299,6632,9862};
    MultiplyShift matfamily = new MultiplyShift(10,seeds);
    
    for(int v=0; v<256; v++){
      int vec[] = new int[10];
      for(int i=0;i<10;i++){
        vec[i] = v + i;
      }
      long hashv[] = matfamily.hash(vec);
      System.out.print(v + ",");
      System.out.print("[");
      for(int i=0;i<seeds.length;i++){
        if(i != 0) System.out.print(", ");
        System.out.print(hashv[i]);
      }
      System.out.println("]");
    }
    
  }
}
