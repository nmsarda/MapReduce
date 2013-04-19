package org.ncsu.mapreduce.datasource.file;

import java.util.ArrayList;

import org.ncsu.mapreduce.common.MapReduceSpecification;
/* Splits the input files into chunks as per as the size given by the user.
 * 
 */
public class FileSplitter implements InputSplit{

	private ArrayList<FileSplitInformation> fileSplitInformation;
	@Override
	public ArrayList<FileSplitInformation> getSplits(MapReduceSpecification spec) {
		// TODO Auto-generated method stub
		fileSplitInformation =  new ArrayList<FileSplitInformation>();
		long minSplitSize = spec.getMinByteSize();
		long splitSize;
		int filesSplit = 0;
		long totalSize = 0;
		FileInformation[] fileInformation = spec.getMapReduceInput().getFiles();
		/* Get the total size of the input */
		while(filesSplit < fileInformation.length)
		{
			totalSize += fileInformation[filesSplit].getFileSize();
			filesSplit++;
		}
		System.out.println("Total size of input: " + totalSize + " bytes\n");
		/* Take the minimum of the split size defined by the user and the
		 * split size achieved by dividing the total size by the number of mappers.
		 * This is done so that each mapper acts on smaller splits.
		 */
		splitSize = Math.min((long) Math.ceil((double) totalSize/spec.getNoOfMappers()), minSplitSize);
		filesSplit = 0;
		while(filesSplit < fileInformation.length)
		{
			splitFile(fileInformation[filesSplit],splitSize,spec);
			filesSplit++;
		}
		int i=0;
	/*	while( i < fileSplitInformation.size())
		{
			System.out.println(fileSplitInformation.get(i).getStart() + "  " + fileSplitInformation.get(i).getLength()+"");
			i++;
		}*/
		return fileSplitInformation;
	}
	/* Do the logical splits and compute the start and the length of each
	 * split. 
	 */
	private void splitFile(FileInformation fileInformation,long splitSize,MapReduceSpecification spec)
	{
		long fileSize = fileInformation.getFileSize();
		long start = 0;
		long length = 0;
		while(fileSize > 0)
		{
			
			if(fileSize < splitSize)
			{
				length = fileSize;
				FileSplitInformation fileSplitInfo = new FileSplitInformation(fileInformation, start, length, spec);
				fileSplitInformation.add(fileSplitInfo);
			}
			else
			{
				length = splitSize;
				FileSplitInformation fileSplitInfo = new FileSplitInformation(fileInformation, start, length, spec);
				fileSplitInformation.add(fileSplitInfo);
			}
			fileSize -= length;
			start += length;
		}
		
	}
	
	
}
