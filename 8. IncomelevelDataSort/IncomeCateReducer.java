import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.Collections;
import java.util.*;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class IncomeCateReducer
        extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {


        Map<String, Double> cateValueMap = new HashMap<>();
        
        int businessSum = 0, zipSum = 0; 
        int businessCount;
        String [] keyValArr; 
        // String[] oneKeyValArr = "xxx:0.777".split(":") ;  
        String[] oneKeyValArr;  
        String finalValueStr = "";   
        String mapStr;
        double valueDouble;  

        for (Text value : values) {
            String[] valueArr = value.toString().split(",");
            businessCount = Integer.parseInt(valueArr[0]);
            businessSum += businessCount;

            mapStr = valueArr[1];
            keyValArr = mapStr.split("/");

            String keySt;
            for(String keyValStr : keyValArr) {
                oneKeyValArr = keyValStr.split(":");
                keySt = oneKeyValArr[0];
                valueDouble = Double.parseDouble(oneKeyValArr[1]);
                if(cateValueMap.containsKey(keySt)) {
                    cateValueMap.put(keySt, (cateValueMap.get(keySt) + valueDouble));
                } else {
                    cateValueMap.put(keySt, valueDouble);
                }

            }            
            zipSum ++;
        }//end of for

        Map cateValueSortedMap = sortByValue(cateValueMap);

        Set<Map.Entry<String, Double>> keyValuesMap = cateValueSortedMap.entrySet(); 
        int firstInt = 0;
        for(Map.Entry<String, Double> kv : keyValuesMap) {
         
            if(firstInt == 0) 
                finalValueStr = kv.getKey()+":"+kv.getValue();
            else 
                finalValueStr = finalValueStr+"/"+kv.getKey()+":"+kv.getValue();

            firstInt ++;             
        }    

        finalValueStr = key.toString()+","+zipSum+","+businessSum+","+finalValueStr; 
        context.write(null, new Text(finalValueStr));

    }

    public static Map sortByValue(Map unsortedMap) {
        Map sortedMap = new TreeMap(new ValueComparator(unsortedMap));
        sortedMap.putAll(unsortedMap);
        return sortedMap;
    }


    static class ValueComparator implements Comparator {
        Map map;

        public ValueComparator(Map map) {
            this.map = map;
        }

        public int compare(Object keyA, Object keyB) {
            Comparable valueA = (Comparable) map.get(keyA);
            Comparable valueB = (Comparable) map.get(keyB);

            if(valueA.equals(valueB)) return 1;
            return valueB.compareTo(valueA);
        }
    }    

}