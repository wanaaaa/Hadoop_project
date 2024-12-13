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

public class ZipCategoryReducer
        extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        List<String> categoryList;
        Map<String, Double> zipCategoryMap = new HashMap<>();

        String finalValueStr = "";
        int countBusiness = 0;
        int categoryNum;
        for (Text value : values) {
            categoryList =new ArrayList<String>(Arrays.asList(value.toString().split(", ")));
            categoryNum = categoryList.size();
            for( String word : categoryList)  {
                if(zipCategoryMap.containsKey(word)) {
                    zipCategoryMap.put(word, zipCategoryMap.get(word) + (1.0/categoryNum));
                } else {
                    zipCategoryMap.put(word, (1.0/categoryNum));
                }
            }//end of for
            countBusiness ++;
        }//end of for

        Map zipCategorySortedMap = sortByValue(zipCategoryMap);

        int firstInt = 0;
        Set<Map.Entry<String, Double>> keyValues = zipCategorySortedMap.entrySet();        
        for(Map.Entry<String, Double> kv : keyValues) {
            if(firstInt == 0) {
                finalValueStr = kv.getKey()+":"+kv.getValue();                
            } else {
                finalValueStr = finalValueStr+"/" + kv.getKey()+":"+kv.getValue();                
            }
            firstInt++;
        } 

        finalValueStr = finalValueStr.replace(",", " ");
        finalValueStr = ","+key.toString()+","+ countBusiness+","+finalValueStr;

        context.write(key, new Text(finalValueStr));

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
///////////////////////////////////////