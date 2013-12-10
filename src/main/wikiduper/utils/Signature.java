package wikiduper.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.WritableComparable;

/**
 * 
 */
public class Signature implements WritableComparable<Signature> {
 
  private long sig[];
  private int length;

  public Signature() {}
  public Signature(int l){
      sig = new long[l];
      length = l;
  }
  public int getLength() {
      return length;
  }
  public void setLength(int length) {
      this.length = length;
  }

  public void set(int i, long l){
      sig[i] = l;
  }
	  
  public long get(int i){
      return sig[i];
  }
  
  public void set(long sig[]){
      this.sig = sig;  
  }
	  
  public long[] get(){
      return sig;
  }
	  
	  
	 /* Deserializes this object.
	 *
	 * @param in source for raw byte representation
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
	  	length = in.readInt();
	  	sig = new long[length];
	  	for(int i=0;i<length;i++){
	  	    sig[i] = in.readLong();
	  	}
	}

	/**
	 * Serializes this object.
	 *
	 * @param out where to write the raw byte representation
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(length);
		for(int i=0;i<length;i++){
		    out.writeLong(sig[i]);
		}
	}

	@Override
	public String toString() {
	  StringBuilder sb = new StringBuilder();
	  //sb.append("Revision Record [");
	  sb.append("[");
	  for(int i=0;i<length;i++){
	      if(i != 0) sb.append(",");
	      sb.append(sig[i]);
	  }
	  sb.append("]");
	  return sb.toString();
    }
     
    
  /**
   * Returns the serialized representation of this object as a byte array.
   *
   * @return byte array representing the serialized representation of this object
   * @throws IOException
   */
  public byte[] serialize() throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(bytesOut);
    write(dataOut);

    return bytesOut.toByteArray();
  }

  /**
   * Creates object from a <code>DataInput</code>.
   *
   * @param in source for reading the serialized representation
   * @return newly-created object
   * @throws IOException
   */
  public static Signature create(DataInput in) throws IOException {
    Signature m = new Signature();
    m.readFields(in);
    return m;
  }

  /**
   * Creates object from a byte array.
   *
   * @param bytes raw serialized representation
   * @return newly-created object
   * @throws IOException
   */
  public static Signature create(byte[] bytes) throws IOException {
    return create(new DataInputStream(new ByteArrayInputStream(bytes)));
  }
  
  @Override
public int compareTo(Signature s) {
    for(int i=0;i<length;i++){
        if(sig[i] < s.get(i)) return -1;
        if(sig[i] > s.get(i)) return 1;
    }
    return 0;

}

@Override
public boolean equals(Object o){
    if(!(o instanceof Signature)) return false;
    return (this.compareTo((Signature) o) == 0);

}

@Override
public int hashCode(){
   return Arrays.hashCode(sig);
}


}
