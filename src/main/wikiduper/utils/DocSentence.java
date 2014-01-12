package wikiduper.utils;



import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * 
 */
public class DocSentence implements WritableComparable {
 
  private long id;
  private long sentence;
  private String language;

  public DocSentence() {}
	public DocSentence(long id, long sentence, String language){
		this.id = id;
		this.sentence = sentence;
		this.language = language;
	}

  
	 /* Deserializes this object.
	 *
	 * @param in source for raw byte representation
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
	  	id = in.readLong();
		sentence = in.readLong();
	  	language = in.readUTF();
	}

	/**
	 * Serializes this object.
	 *
	 * @param out where to write the raw byte representation
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(id);
		out.writeLong(sentence);
		out.writeUTF(language);
	}

	@Override
	public String toString() {
	  StringBuilder sb = new StringBuilder();
	  //sb.append("Revision Record [");
	  sb.append("[");
	  sb.append(id + ",");
	  sb.append(sentence + ",");
	  sb.append(language);
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
  public static DocSentence create(DataInput in) throws IOException {
    DocSentence m = new DocSentence();
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
  public static DocSentence create(byte[] bytes) throws IOException {
    return create(new DataInputStream(new ByteArrayInputStream(bytes)));
  }

  public long getId() {
      return id;
  }
  public void setId(long id) {
      this.id = id;
  }
public long getSentence() {
    return sentence;
}
public void setSentence(long sentence) {
    this.sentence = sentence;
}
public String getLanguage() {
    return language;
}
public void setLanguage(String language) {
    this.language = language;
}

@Override
public int hashCode(){
    String str = id + "," + sentence + "," + language;
    return str.hashCode();
}
@Override
public boolean equals(Object o){
    if(!(o instanceof DocSentence)){
        return false;
    }
    DocSentence otherdoc = (DocSentence) o;
    return (otherdoc.id==this.id) && otherdoc.language.equals(this.language) && (otherdoc.sentence==this.sentence);
}

@Override
public int compareTo(Object o) {
    if(!(o instanceof DocSentence)){
        return 0;
    }
    DocSentence ds = (DocSentence) o;
    if(language.compareTo(ds.getLanguage()) == 0){
        if(Long.compare(id, ds.getId()) == 0){
            return Long.valueOf(sentence).compareTo(getSentence());
        }else{
            return Long.valueOf(id).compareTo(ds.getId());
        }
    }else{
        return language.compareTo(ds.getLanguage());
    }

}

}
