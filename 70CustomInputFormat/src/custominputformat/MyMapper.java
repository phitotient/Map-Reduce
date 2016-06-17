package custominput;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<MyKey, MyValue, Text, Text> {
        
          protected void map(MyKey key, MyValue value, Context context)
              throws java.io.IOException, InterruptedException {
        	  
           if(value.getAmt()>150)
           {
        	   context.write(new Text(key.getTid()), new Text(value.getUpperCat()));
           }
            		
          }  
}