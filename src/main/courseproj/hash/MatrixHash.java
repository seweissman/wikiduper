package courseproj.hash;

import java.util.Random;


/**
 * Folowing the scheme found here;
 * http://www.cs.cmu.edu/~avrim/Randalgs97/lect0127
 *
 * Creates a family of hashes using a random matrix of mxn bits.
 * 
 * The matrix is initialized on startup so the hash size m -> n must
 * be specified in the constructor.
 * 
 * The family H is initialized with a vector of random seeds, one for each
 * hash function. (So |H| = |seeds|.)
 * 
 * Each call to hash(x) returns a vector v here v_i = h_i(x).
 *
 */
public class MatrixHash {

  /*
   * 
  http://www.cs.cmu.edu/~avrim/Randalgs97/lect0127
Way #1: Let's assume that M, N are powers of 2: M = {0,1}^m and N = {0,1}^n.

H = { m x n 0-1 matrices}.  h(x) = hx, in GF2 (addition is mod 2).

Note: h(0) = 0. So, could remove 0 from domain if want strong 2-universal.

Claim: For x!=y, Pr[h(x) = h(y)] = 1/|N|.
Proof: Say x and y differ in ith coordinate, and say that y_i = 1.
  Let z = y, but with z_i = 0. (So, it may be that z=x).
  Then, h(y) = h(z) + h_i, where h_i is the ith column of h.
  Notice that h(z) is independent from h_i, So, 
  Pr[h(y)=h(x)] = Pr[h_i = h(x) - h(z)] = 1/2^n.

In fact, we've shown that for any r, prob[h(y) = h(x) + r] = 1/N,
independent of h(x).  So, so long as x!=0, we have strong 2-universality.

So, this is a nice, easy family of hash functions.  One problem is
that it requires log(N)*log(M) random bits.


   * 
   */
  
  boolean rmatrix[][][];
  long []seeds;
  int m;
  int n;
  // hash m sized things to n sized things
  /**
   * 
   * @param m hash input size
   * @param n has output size
   * @param seeds One random seed for each hash fn in family.
   */
  public MatrixHash(int m, int n, long seeds[]){
    rmatrix = new boolean[m][n][seeds.length];
    this.seeds = seeds;
    this.m = m;
    this.n = n;
    for(int s=0; s<seeds.length;s++){
      long seed = seeds[s];
      Random r = new Random(seed);
      for(int i = 0; i<m; i++){
        for(int j = 0; j<n; j++){
          rmatrix[i][j][s] = r.nextBoolean(); 
        }
      }
    }
  }
  
  
  /**
   * Hashes a vector of longs where v represents a packed input.
   * Note: Requires 64*v.length <= m
   * 
   * Returns a vector of values w where w_i = h_i(v).
   * 
   * @param v
   * @return
   */
  public long[] hash(long v[]){
    // TODO: throw error if v is too long
    long hashvec[] = new long[seeds.length];
    long x = 0;
    for(int s=0;s<seeds.length;s++){
    long hout = 0;
    for(int i=0;i<n;i++){
      int j = 0;
      while(j < m){
        if(rmatrix[j][i][s]){
          x ^= (v[j/64]>>(j%64))&1;
        }
        j++;
      }
      hout ^= x<<i;
    }
    hashvec[s] = hout; 
    }
    return hashvec;
  }
  
  public static void main(String args[]){
    long seeds[] = {2343,2299,6632,9862};
    int l = 2;
    MatrixHash matfamily = new MatrixHash(128,16,seeds);
    for(long v=0; v<256; v++){
      long vec[] = new long[10];
      for(int i=0;i<l;i++){
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
