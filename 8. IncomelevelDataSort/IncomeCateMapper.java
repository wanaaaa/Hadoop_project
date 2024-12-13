import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.Arrays;
import java.util.List;

public class IncomeCateMapper extends
    Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
       
        String line = value.toString();
        String[] lineArr = line.split(",");
        double incomeDouble =Double.parseDouble(lineArr[1]) ;
        String keyStr;
        if(incomeDouble <= 30000) keyStr = "<30";
        else if(incomeDouble <= 40000) keyStr = "30_40";
        else if(incomeDouble <= 45000) keyStr = "40_45";
        else if(incomeDouble <= 5000) keyStr = "45_50";
        else if(incomeDouble <= 55000) keyStr = "50_55";
        else if(incomeDouble < 60000) keyStr = "55_60";
        else if(incomeDouble < 70000) keyStr = "60_70";
        else keyStr = ">70";

        String valueStr = lineArr[2]+","+lineArr[3];

        context.write(new Text(keyStr), new Text(valueStr) );
        
            
    }
}