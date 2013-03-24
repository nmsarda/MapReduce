package WordCount;
import org.ncsu.mapreduce.common.*;
import org.ncsu.mapreduce.datasource.database.DBConnectionParameters;
public class WordCountMain {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		
		
		MapReduceSpecification spec = new  MapReduceSpecification();
		DBConnectionParameters dbConn = new DBConnectionParameters();
			dbConn.setDbDriver("oracle.jdbc.driver.OracleDriver");
			dbConn.setJdbcURL("jdbc:oracle:thin:@ora.csc.ncsu.edu:1521:orcl");
			dbConn.setUserName("nmsarda");
			dbConn.setPassword("001080892");
		
		MapReduceInput inp = spec.add_input();		
			inp.setTableName("test");
			inp.setMapperClass("WordCount.Mapper");
			inp.setFormat("textinput");
			inp.setDbConnectionParameters(dbConn);
			inp.setNumberOfFiles(1);
		
		MapReduceOutput op = spec.output();
			op.setOutputFileDirectory("Reducer");
			op.setReducerClass("WordCount.Reducer");
			
		spec.setNoOfThreads(10);
		spec.minByteSize(5);
		spec.setNoOfMappers(2);
		
		JobRunner job = new JobRunner();
		job.run(spec);
		System.out.println(spec.getMapReduceInput().getFiles()[0].getFileSize());
		System.out.println("Done!");
	}

}
