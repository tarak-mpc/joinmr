package join.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Composite Key
 *
 */
public class CompositeKey implements WritableComparable<CompositeKey>
{
  private String id;
  private String tag;

  public CompositeKey(){
  }

  public CompositeKey(String id, String tag){
    this.id = id;
    this.tag = tag;
  }

  public void readFields(DataInput in) throws IOException {
    this.id = WritableUtils.readString(in);
    this.tag = WritableUtils.readString(in);
  }

  public void write(DataOutput out) throws IOException {
    WritableUtils.writeString(out, id);
    WritableUtils.writeString(out, tag);
  }

  public int compareTo(CompositeKey ck) {
    int cmp = id.compareTo(ck.id);
    if (cmp != 0){
      return cmp;
    }
    return tag.compareTo(ck.tag);
  }

  public String getId(){
    return id;
  }

  public String getTag(){
    return tag;
  }

  public void setId(String id){
    this.id = id;
  }

  public void setTag(String tag){
    this.tag = tag;
  }

}