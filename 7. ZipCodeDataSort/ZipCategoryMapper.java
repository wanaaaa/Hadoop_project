import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.Arrays;
import java.util.List;

public class ZipCategoryMapper extends
    Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
       
        String line = value.toString();
        int zipFp, zipLp, categoriesFp, categoriesLp;
        String zipStr, categoriesStr;
        
        zipFp = line.indexOf("\", \"postal_code\": \"")+19;
        zipLp = line.indexOf("\"latitude\": ")-3;
        zipStr = line.substring(zipFp, zipLp);
        
        categoriesFp = line.indexOf(", \"categories\": [")+17;
        categoriesLp = line.indexOf(", \"hours\": {")-1;
        categoriesStr = line.substring(categoriesFp, categoriesLp);
        categoriesStr = categoriesStr.replace("\"", "");
        categoriesStr = categoriesStr.replace("/", " ");
        categoriesStr = categoriesStr.replace(":", " ");
        categoriesStr = categoriesStr.replace("\t", " ");

        if(zipStr.matches("\\d+") && zipStr.length()==5) {
            context.write(new Text(zipStr), new Text(categoriesStr) );
        }                   
    }
}