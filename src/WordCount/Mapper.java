package WordCount;

import java.util.ArrayList;
import java.util.Scanner;
import java.util.regex.Pattern;

import org.ncsu.mapreduce.common.KeyValueClass;

public class Mapper {
	
	public ArrayList<KeyValueClass<String, Integer>> map(String t){
		ArrayList<KeyValueClass<String, Integer>> list = new ArrayList<>();
		
		String[] temp = t.split(" ");
		for(int i=0; i<temp.length; i++){
			Scanner word = new Scanner(temp[i]);
			word.useDelimiter(Pattern.compile("\\p{Space}|\\p{Punct}"));
			while(word.hasNext()){
				String t1 = word.next();
				list.add(new KeyValueClass<String, Integer>(t1, 1));
			}				
			word.close();
		}
		return list;
	}

}
