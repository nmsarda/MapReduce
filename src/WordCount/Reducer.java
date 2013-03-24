package WordCount;

import java.util.ArrayList;

public class Reducer {
	
	public String reduce(String key, ArrayList<String> values){
		int count = 0;
		for(String value : values){
			count += Integer.parseInt(value);
		}
		return Integer.toString(count);
	}

}
