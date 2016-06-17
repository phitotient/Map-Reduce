package custominput;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class MyKey implements WritableComparable{
	private Text tid,txndate;
	
	public MyKey(){
		this.tid = new Text();
		this.txndate = new Text();
	}
	public MyKey(Text tid,Text txndate){
		this.tid = tid;
		this.txndate = txndate;		
	}
	public void readFields(DataInput in) throws IOException{
		tid.readFields(in);
		txndate.readFields(in);
	}
	
	public void write(DataOutput out) throws IOException{
		tid.write(out);
		txndate.write(out);
	}

	public int compareTo(Object o){
		MyKey other = (MyKey)o;
		int cmp = tid.compareTo(other.tid);
		if(cmp != 0){
				return cmp;
		}
		return cmp = txndate.compareTo(other.txndate);
	}
	public String getTid() {
		return tid.toString();
	}
	public void setTid(Text tid) {
		this.tid = tid;
	}
	public String getTxndate() {
		return txndate.toString();
	}
	public void setTxndate(Text txndate) {
		this.txndate = txndate;
	}
	
	
	

}
