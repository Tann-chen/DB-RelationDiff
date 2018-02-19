import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class RelationDiffByte {

	public static boolean DEBUG = true;
	public static int maxNumOfTuple;
	public static int expectedBlocksInMemory;
	public static final float K = 1024.0f;
	public static final int TUPLESPB = 40;
	public static final int TUPLEBYTE = 101;
	public static final int BLOCKSIZE = TUPLESPB * TUPLEBYTE;// one block has 40 tuples, 101byte for one tuple
	public static float RUNNINGMEMORY = 1290f; // kb
	public static float RUNNINGMEMORY_PHASE2 = RUNNINGMEMORY+40; 	//kb
	public static float RUNNINGMEMORY_DIFF = RUNNINGMEMORY+40;

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
			System.out.println("*******************"+ outputPrefix + " PHASE ONE*********************************");
			System.out.println("Max tuple to fill:"+maxNumOfTuple);
		}

		byte[][] sublistbyte = new byte[maxNumOfTuple][TUPLEBYTE];
		if(DEBUG) {
			System.out.println("Free Memory:" + Runtime.getRuntime().freeMemory() / K + "KB");
		}
		try {
			FileInputStream ios = new FileInputStream(inputFile);
			while (eof != -1) {
				eof = ios.read(sublistbyte[lines++]);
				iocost++;
				if (lines == maxNumOfTuple || eof == -1) {
					if(eof == -1) {
						lines--;
					}
					// sort the sublist
					QuickSortTupleByte.quicksort(sublistbyte, 0, lines - 1);
					// Arrays.sort(sublistbyte, 0, lines, new Comparator<byte[]>() { // todo:i-1
					// @Override
					// public int compare(byte[] t1, byte[] t2) {
					// for (int i = 0; i < 8; i++) {
					// if (t1[i] == t2[i]) {
					// continue;
					// } else {
					// return t1[i] - t2[i];
					// }
					// }
					// return 0;
					// }
					// });
					FileOutputStream out = new FileOutputStream(outputPrefix + "_" + sublistCount++ + ".txt");
					int lineCount = 0;
					for (int j = 0; j < lines; j++) {
						out.write(sublistbyte[j], 0, TUPLEBYTE - 1);
						iocost++;
						lineCount++;
						if (lineCount == TUPLESPB) {
							lineCount = 0;
							out.write('\n');
						}
					}
					lines = 0;
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
	 * The method is to implement the process of phase 2
	 */
	public static void phaseTwo(String outputPrefix, int sublistCount) {
		System.gc();
		float startMemory = Runtime.getRuntime().freeMemory() / K;
		long startTime = System.currentTimeMillis();
		// need one output block-size buffer
		int inputBufferBlocks = (int) Math.floor((Runtime.getRuntime().freeMemory() - RUNNINGMEMORY_PHASE2 * K) / BLOCKSIZE) - 1;
		if(DEBUG) {
			System.out.println("++++++++++++++++++++++"+ outputPrefix + " PHASE TWO+++++++++++++++++++++++++++++++++");
			System.out.println("Max Blocks to fill:"+inputBufferBlocks);
		}
		// average blocks for every sublist
		int avgDistributedBlocks = (int) Math.floor(inputBufferBlocks / sublistCount);
		// declare output buffer
		byte[][][] inputBuffer = new byte[sublistCount][avgDistributedBlocks * TUPLESPB][TUPLEBYTE-1];

		int tuplesCount[] = new int[sublistCount];
		int sublistEof[] = new int[sublistCount];
		FileInputStream[] mergeInput = new FileInputStream[sublistCount];
		try {
			for (int i = 0; i < sublistCount; i++) {
				mergeInput[i] = new FileInputStream(outputPrefix + "_" + i + ".txt");
			}
			boolean finish=false;
			int phase2_step = 0;
			FileOutputStream out = new FileOutputStream(outputPrefix + "_sorted.txt");
			while(!finish) {
				//tuplesCount = new int[sublistCount];
				for (int i = 0; i < sublistCount; i++) {
					int tuples = 0;
					while (sublistEof[i] != -1) {
						sublistEof[i] = mergeInput[i].read(inputBuffer[i][tuples++]);
						iocost++;
						if(sublistEof[i] == -1) {
							tuples--;
							tuplesCount[i] = tuples;
							break;
						} else if (tuples % TUPLESPB == 0) {
							mergeInput[i].skip(1);
						}
						tuplesCount[i] = tuples;
						if (tuples / TUPLESPB == avgDistributedBlocks) {
							break;
						}
					}

				}
				int sum = 0;
				for (int i = 0; i < sublistCount; i++) {
					sum+=tuplesCount[i];
				}
				if(sum == 0) {
					break;
				}
				if(DEBUG) {
					recordTime(startTime, outputPrefix + " phase 2-"+phase2_step+" time fill");
					recordMemory(startMemory, outputPrefix + " phase 2-"+phase2_step+" time fill");
				}
				// merge
				int outputBufferOffset = 0;
				int[] sublistIndex = new int[sublistCount];
				byte[][] outputBuffer = new byte[TUPLESPB][];
				int thisSublist = 0;
				boolean writeFinish = false;
				while (!writeFinish) {
					// compare to find smallest
					writeFinish = true;
					for (int j = 0; j < sublistCount; j++) {
						if(sublistIndex[j] < tuplesCount[j]) {
							outputBuffer[outputBufferOffset] = inputBuffer[j][sublistIndex[j]];
							thisSublist = j;
							writeFinish = false;
							break;
						}
					}
					if(writeFinish) {
						break;
					}
					for (int j = 0; j < sublistCount; j++) {
//						System.out.println(new String(inputBuffer[j][sublistIndex[j]])+new String(outputBuffer[outputBufferOffset]));
						if (sublistIndex[j] < tuplesCount[j] && compare(inputBuffer[j][sublistIndex[j]], outputBuffer[outputBufferOffset]) < 0) {
							outputBuffer[outputBufferOffset] = inputBuffer[j][sublistIndex[j]];
							thisSublist = j;
						}
//						for (int i = 0; i < tuplesCount[j]; i++) {
//							out.write(inputBuffer[j][i], 0, TUPLEBYTE - 1);
//							out.write('\n');
//						}
					}
					outputBufferOffset++;
					sublistIndex[thisSublist]++;
					writeFinish = true;
					for (int i = 0; i < sublistCount; i++) {
						if(sublistIndex[i] != tuplesCount[i]) {
							writeFinish = false;
							break;
						}
					}
					if (outputBufferOffset == TUPLESPB || writeFinish) {
						// write back
						for (int j = 0; j < outputBufferOffset; j++) {
							out.write(outputBuffer[j], 0, TUPLEBYTE - 1);
							out.write('\n');
							iocost++;
						}
						outputBufferOffset = 0;
					}
				}
				sublistIndex = null;
				outputBuffer = null;
				finish = true;
				for (int i = 0; i < sublistCount; i++) {
					if(sublistEof[i] != -1) {
						finish = false;
						break;
					}
				}
				phase2_step++;
			}
			out.close();
			out = null;
			for (int i = 0; i < sublistCount; i++) {
				mergeInput[i].close();
				mergeInput[i] = null;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		inputBuffer = null;
		tuplesCount = null;
		sublistEof = null;
		mergeInput = null;
		System.gc();
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


	public static void compareDiff(String t1Prefix, String t2Prefix){
		System.gc();
		float startMemory = Runtime.getRuntime().freeMemory() / K;
		long startTime = System.currentTimeMillis();
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
			byte[][] t1Buffer = new byte[avgDistributedBlocks * TUPLESPB][TUPLEBYTE-1];
			//t2
			byte[][] t2Buffer = new byte[avgDistributedBlocks * TUPLESPB][TUPLEBYTE-1];

			int lenOft1Buffer;
			int lenOft2Buffer;

			//first load
			lenOft1Buffer = loadFile(t1Buffer,ios1);
			lenOft2Buffer = loadFile(t2Buffer,ios2);

			//compare
			int currentT1 = 0;	//pointer
			int currentT2 = 0;

			while (currentT1 < lenOft1Buffer && currentT2 < lenOft2Buffer){
				if(compare(t1Buffer[currentT1], t2Buffer[currentT2]) > 0){
					byte[] tempT2 = t2Buffer[currentT2];
					out.write(tempT2);
					out.write((byte)0);
					out.write('\n');
					currentT2 ++;
					tempT2 = null;
				}
				else if(compare(t1Buffer[currentT1], t2Buffer[currentT2]) < 0){
					byte[] tempT1 = t1Buffer[currentT1];
					int counter = 1;
					while (compare(tempT1,t1Buffer[++currentT1]) == 0){
						counter++;
					}
					out.write(tempT1);
					out.write((byte)counter);
					out.write('\n');
					currentT1++;

					tempT1 = null;
					counter = 0;
				}
				else{	//equal
					byte[] tempT1 = t1Buffer[currentT1];
					int counter1 = 1;
					while (compare(tempT1,t1Buffer[++currentT1]) == 0){
						counter1++;
					}

					byte[] tempT2 = t2Buffer[currentT2];
					int counter2 = 1;
					while (compare(tempT2,t2Buffer[++currentT2]) == 0){
						counter2++;
					}

					out.write(tempT1);
					if(counter1 > counter2){
						out.write((byte)(counter1-counter2));
					}else{
						out.write((byte)0);
					}
					out.write('\n');
					currentT1++;
					currentT2++;

					tempT1 = null;
					tempT2 = null;
					System.gc();
				}



				//check exhausted buffer case
				if(currentT1 == lenOft1Buffer){
					lenOft1Buffer =loadFile(t1Buffer,ios1);
					currentT1 = 0;

					//trick
					if(lenOft1Buffer < t1Buffer.length){
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
