package custominput;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class MyValue implements WritableComparable{
	private Text uid,amt,cat;
	
	public MyValue(){
		this.uid = new Text();
		this.amt = new Text();
		this.cat = new Text();
	}
	public MyValue(Text value1,Text value2,Text value3){
		this.uid = value1;
		this.amt = value2;
		this.cat = value3;
	}
	public void readFields(DataInput in) throws IOException{
		uid.readFields(in);
		amt.readFields(in);
		cat.readFields(in);
	}
	
	public void write(DataOutput out) throws IOException{
		uid.write(out);
		amt.write(out);
		cat.write(out);
	}
	public int compareTo(Object o){
		MyValue other = (MyValue)o;
		int cmp = amt.compareTo(other.amt);
		if(cmp != 0){
				return cmp;
		}
		return cat.compareTo(other.cat);
		
	}
	
	public String getUpperCat() {
		return cat.toString().toUpperCase();
	}
	
	
	public Text getUid() {
		return uid;
	}
	public void setUid(Text uid) {
		this.uid = uid;
	}
	public Double getAmt() {
		return Double.parseDouble(amt.toString());
	}
	public void setAmt(Text amt) {
		this.amt = amt;
	}
	public String getCat() {
		return cat.toString();
	}
	public void setCat(Text cat) {
		this.cat = cat;
	}
	
	public Double get10xAmt() {
		return Double.parseDouble(amt.toString())*10;
	}

}
