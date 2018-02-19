import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class RelationDiffByteCopy {

	public static boolean DEBUG = true;
	public static int maxNumOfTuple;
	public static int expectedBlocksInMemory;
	public static final float K = 1024.0f;
	public static final int TUPLESPB = 40;
	public static final int TUPLEBYTE = 100;
	public static final int BLOCKSIZE = TUPLESPB * TUPLEBYTE;// one block has 40 tuples, 101byte for one tuple
	public static float RUNNINGMEMORY = 1290f; // kb
	public static float RUNNINGMEMORY_PHASE2 = RUNNINGMEMORY+40; 	//kb
	public static float RUNNINGMEMORY_DIFF = RUNNINGMEMORY;		//kb

	public static int iocost = 0;
	
	public static void main(String[] args) {
		System.out.println("Total Memory:" + Runtime.getRuntime().totalMemory() / (K * K) + "MB");
		float freeMemory = Runtime.getRuntime().freeMemory() / (K * K);
		System.out.println("Free Memory:" + freeMemory + "MB");
		if(freeMemory > 5) {
			RUNNINGMEMORY *= 1.4;
			RUNNINGMEMORY_PHASE2 = RUNNINGMEMORY*0.8f;
		}
		expectedBlocksInMemory = (int) Math.floor((Runtime.getRuntime().freeMemory() - RUNNINGMEMORY * K) / BLOCKSIZE);
		maxNumOfTuple = TUPLESPB * expectedBlocksInMemory;
		long startTime = System.currentTimeMillis();
		int t1SubLists = phaseOne("E:\\bag1.txt", "t1");
		phaseTwo("t1", t1SubLists);
		freeMemory = Runtime.getRuntime().freeMemory() / (K * K);
		if(freeMemory > 5) {
			expectedBlocksInMemory = (int) Math.floor((Runtime.getRuntime().freeMemory() - RUNNINGMEMORY * K) / BLOCKSIZE);
			maxNumOfTuple = TUPLESPB * expectedBlocksInMemory;
		}
		int t2SubLists = phaseOne("E:\\bag2.txt", "t2");
		if(freeMemory > 5) {
			RUNNINGMEMORY_PHASE2*=1.7;
			expectedBlocksInMemory = (int) Math.floor((Runtime.getRuntime().freeMemory() - RUNNINGMEMORY * K) / BLOCKSIZE);
			maxNumOfTuple = TUPLESPB * expectedBlocksInMemory;
		}
		phaseTwo("t2", t2SubLists);
		recordTime(startTime, "Total");
		System.out.println("Total I/O Cost is:" + iocost + " times R/W");
	}


	/**
	 * The input is T1 and T2 input file, the output is sorted file
	 */
	public static int phaseOne(String inputFile, String outputPrefix) {
		float startMemory = Runtime.getRuntime().freeMemory() / K;
		long startTime = System.currentTimeMillis();
		int sublistCount = 0;
		int eof = 0;
		int lines = 0;
		if(DEBUG) {
			System.out.println("*******************"+ outputPrefix + " PHASE ONE *********************************");
			System.out.println("Max tuple to fill:" + maxNumOfTuple);
		}

		byte[][] sublistbyte = new byte[maxNumOfTuple][TUPLEBYTE];
		if(DEBUG) {
			System.out.println("Free Memory:" + Runtime.getRuntime().freeMemory() / K + "KB");
		}

		try {
			FileInputStream ios = new FileInputStream(inputFile);
			while (eof != -1) {
				eof = ios.read(sublistbyte[lines]);
				ios.skip(1); 	//skip every '\n' end of lines
				lines++;

				//one block one io
				if(lines % TUPLESPB == 0 || eof == -1){
					iocost++;
				}

				//sorting
				if (lines == maxNumOfTuple || eof == -1) {
					// sort the sublist
					QuickSortTupleByte.quicksort(sublistbyte, 0, lines - 1);
					FileOutputStream out = new FileOutputStream(outputPrefix + "_" + sublistCount++ + ".txt");
					int lineCount = 0;
					for (int j = 0; j < lines; j++) {
						out.write(sublistbyte[j], 0, TUPLEBYTE);
						lineCount++;
						if (lineCount == TUPLESPB) {
							lineCount = 0;
							out.write('\n');
							iocost++;
						}
					}

					//gc
					lines = 0;
					lineCount = 0;
					out.close();
					out = null;
					if(DEBUG) {
						System.out.println("Finish " + sublistCount + "sublist, Free Memory:" + Runtime.getRuntime().freeMemory() / K + "KB");
					}
				}
			}
			ios.close();
			ios = null;
		} catch (IOException e) {
			e.printStackTrace();
		}
		if(DEBUG) {
			recordTime(startTime, outputPrefix + " phase 1");
			recordMemory(startMemory, outputPrefix + " phase 1");
		}

		sublistbyte = null;
		System.gc();
		return sublistCount;
	}


	/**
	 * The method is to implement the process of phase 2
	 */
	public static void phaseTwo(String outputPrefix, int sublistCount) {
		float startMemory = Runtime.getRuntime().freeMemory() / K;
		long startTime = System.currentTimeMillis();

		// need one output block-size buffer
		int inputBufferBlocks = (int) Math.floor((Runtime.getRuntime().freeMemory() - RUNNINGMEMORY_PHASE2 * K) / BLOCKSIZE);

		if(DEBUG) {
			System.out.println("++++++++++++++++++++++"+ outputPrefix + " PHASE TWO +++++++++++++++++++++++++++++++++");
			System.out.println("Max Blocks to fill:"+inputBufferBlocks);
		}

		// average blocks for every sublist
		int avgDistributedBlocks = (int) Math.floor(inputBufferBlocks / sublistCount);
		// declare output buffer
		byte[][][] inputBuffer = new byte[sublistCount][avgDistributedBlocks * TUPLESPB][TUPLEBYTE];
		int[] lengthOfBuffer = new int[sublistCount];
		int[] currentIndex = new int[sublistCount];
		boolean[] eofFlag = new boolean[sublistCount];
		int endCounter = 0;

		FileInputStream[] mergeInput = new FileInputStream[sublistCount];

		try {
			for (int i = 0; i < sublistCount; i++) {
				mergeInput[i] = new FileInputStream(outputPrefix + "_" + i + ".txt");
			}
			FileOutputStream out = new FileOutputStream(outputPrefix + "_sorted.txt");


			//first load
			for(int i=0; i<sublistCount; i++){
				lengthOfBuffer[i] = loadFile(inputBuffer[i],mergeInput[i]);
				currentIndex[i] = 0;
				eofFlag[i] = false;
			}

			//merge
			while (true){
				//find smallest
				byte[] smallest = new byte[100];
				int smallestIndex = 0;
				Arrays.fill(smallest,Byte.MAX_VALUE);

				for(int i=0; i<sublistCount; i++){
					if(eofFlag[i]){
						continue;
					}

					if(compare(smallest,inputBuffer[i][currentIndex[i]]) > 0){
						smallest = inputBuffer[i][currentIndex[i]];
						smallestIndex = i;
					}
				}

				//write the smallest into output
				out.write(smallest);
				out.write('\n');
				currentIndex[smallestIndex] += 1;

				// if this buffer is empty
				if(currentIndex[smallestIndex] == lengthOfBuffer[smallestIndex] && !eofFlag[smallestIndex]){
					lengthOfBuffer[smallestIndex] = loadFile(inputBuffer[smallestIndex],mergeInput[smallestIndex]);
					currentIndex[smallestIndex] = 0;

					if(lengthOfBuffer[smallestIndex] == 0){		//end of the sublist
						eofFlag[smallestIndex] = true;
						endCounter++;
					}

					if(endCounter == sublistCount){
						System.out.println("+++++++++++++++++++++++ END OF MERGE +++++++++++++++++++++++++++++++");
						break;
					}

				}

				//gc
				smallest = null;
				smallestIndex = 0;
				System.gc();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * compare the diff between two sorted relations
	 */
	public static void compareDiff(String t1Prefix, String t2Prefix){
		System.gc();

		int inputBufferBlocks = (int) Math.floor((Runtime.getRuntime().freeMemory() - RUNNINGMEMORY_DIFF * K) / BLOCKSIZE);
		if(DEBUG) {
			System.out.println("=========================== COMPARE DIFF ===========================");
			System.out.println("Max Blocks to fill:"+inputBufferBlocks);
		}

		try {
			FileInputStream ios1 = new FileInputStream(t1Prefix + "_sorted.txt");
			FileInputStream ios2 = new FileInputStream(t2Prefix + "_sorted.txt");
			FileOutputStream out = new FileOutputStream("result.txt");

			int avgDistributedBlocks = (int) Math.floor(inputBufferBlocks / 2);

			//t1
			byte[][] t1Buffer = new byte[avgDistributedBlocks * TUPLESPB][TUPLEBYTE];
			//t2
			byte[][] t2Buffer = new byte[avgDistributedBlocks * TUPLESPB][TUPLEBYTE];

			//record how much tuple in memory buffer
			int lenOft1Buffer;
			int lenOft2Buffer;

			//first load
			lenOft1Buffer = loadFile(t1Buffer,ios1);
			lenOft2Buffer = loadFile(t2Buffer,ios2);

			//compare
			int currentT1 = 0;	   //pointer
			int currentT2 = 0;

			while (currentT1 < lenOft1Buffer && currentT2 < lenOft2Buffer){
				if(compare(t1Buffer[currentT1], t2Buffer[currentT2]) > 0){
					byte[] tempT2 = t2Buffer[currentT2];
					currentT2 += 1;
					while (currentT2 < lenOft2Buffer && compare(tempT2,t2Buffer[currentT2]) == 0){
						currentT2++;
					}
					out.write(tempT2);
					out.write((byte)0);
					out.write('\n');
					tempT2 = null;
				}
				else if(compare(t1Buffer[currentT1], t2Buffer[currentT2]) < 0){
					byte[] tempT1 = t1Buffer[currentT1];
					int counter = 1;
					currentT1 += 1;
					while (currentT1 < lenOft1Buffer && compare(tempT1,t1Buffer[currentT1]) == 0){
						counter++;
						currentT1++;
					}
					out.write(tempT1);
					out.write((byte)counter);
					out.write('\n');

					tempT1 = null;
					counter = 0;
				}
				else{	//equal
					byte[] tempT1 = t1Buffer[currentT1];
					int counter1 = 1;
					currentT1 += 1;
					while (currentT1 < lenOft1Buffer && compare(tempT1,t1Buffer[currentT1]) == 0){
						counter1++;
						currentT1++;
					}

					byte[] tempT2 = t2Buffer[currentT2];
					int counter2 = 1;
					currentT2 += 1;
					while (currentT2 < lenOft2Buffer && compare(tempT2,t2Buffer[currentT2]) == 0){
						counter2++;
						currentT2++;
					}

					out.write(tempT1);
					if(counter1 > counter2){
						out.write((byte)(counter1-counter2));
					}else{
						out.write((byte)0);
					}
					out.write('\n');

					tempT1 = null;
					tempT2 = null;
					counter1 = 0;
					counter2 = 0;
				}

				//gc
				System.gc();

				//check exhausted buffer case
				if(currentT1 == lenOft1Buffer){
					lenOft1Buffer =loadFile(t1Buffer,ios1);
					currentT1 = 0;

					//trick
					if(lenOft1Buffer < t1Buffer.length){		//end of file
						byte[] biggest = new byte[100];
						Arrays.fill(biggest,Byte.MAX_VALUE);
						t1Buffer[lenOft1Buffer] = biggest;
						lenOft1Buffer += 1;
					}
				}

				if(currentT2 == lenOft2Buffer){
					lenOft2Buffer = loadFile(t2Buffer,ios2);
					currentT2 = 0;

					//trick
					if(lenOft2Buffer < t2Buffer.length){
						byte[] biggest = new byte[100];
						Arrays.fill(biggest,Byte.MAX_VALUE);
						t2Buffer[lenOft2Buffer] = biggest;
						lenOft2Buffer += 1;
					}
				}
			}

		}catch (IOException e){
			System.out.println(e.getMessage());
		}
	}



	public static void recordTime(long startTime, String function) {
		long endTime = System.currentTimeMillis();
		long duration = (endTime - startTime);
		System.out.println(function + " time is:" + duration + " milliseconds");
	}

	public static void recordMemory(float start, String funciont) {
		float used = start - Runtime.getRuntime().freeMemory() / K;
		System.out.println("Memory used:" + used + "KB");
		System.out.println("Total Memory:" + Runtime.getRuntime().totalMemory() / (K * K) + "MB");
		System.out.println("Free Memory:" + Runtime.getRuntime().freeMemory() / (K * K) + "MB\n");
	}


	/**
	 * compare the diff between ID of two tuple
	 */
	public static int compare(byte[] t1, byte[] t2) {
		for (int i = 0; i < 8; i++) {
			if (t1[i] == t2[i]) {
				continue;
			} else {
				return t1[i] - t2[i];
			}
		}

		return 0;
	}


	/**
	 *  return the length of tuple in memory buffer, return 0 if end of file
	 */
	public static int loadFile(byte[][] buffer, FileInputStream ios) throws IOException{
		int eof = 0;
		int i = 0;
		for(; i<buffer.length; i++){
			if(eof == -1){
				break;
			}
			eof = ios.read(buffer[i]);
		}

		return i;
	}
}
