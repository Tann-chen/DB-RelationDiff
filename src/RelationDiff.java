import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class RelationDiff {

	public static boolean DEBUG = true;
	public static int MAX_TUPLES;
	public static int MAX_BLOCKS;
	public static final float K = 1024.0f;
	public static final int TUPLES_OF_BLOCK = 40;
	public static final int BYTES_OF_TUPLE = 101;
	public static final int BLOCK_SIZE = TUPLES_OF_BLOCK * BYTES_OF_TUPLE;// one block has 40 tuples, 101byte for one tuple
	public static final byte[] MAX_ID = "99999999Lelia     Lopez     110008560319893Peep Key,Neffini,YT,X0B 0J4                              \n".getBytes();
	public static float RUNNING_MEMORY = 1024; // kb
	public static int iocost = 0;

	public static byte[] extra1 = "(".getBytes();
	public static byte[] extra2 = ",".getBytes();
	public static byte[] extra3 = ")\n".getBytes();
	public static int extraLength = extra1.length + extra2.length + extra3.length;

	
	public static void main(String[] args) {
		System.out.println("Total Memory:" + Runtime.getRuntime().totalMemory() / (K * K) + "MB");
		float freeMemory = Runtime.getRuntime().freeMemory() / (K * K);
		System.out.println("Free Memory:" + freeMemory + "MB");
		if(freeMemory > 7) {
			RUNNING_MEMORY *= 3.4;
		} else {
			RUNNING_MEMORY *= 1.7;
		}
		MAX_BLOCKS = (int) Math.floor((Runtime.getRuntime().freeMemory() - RUNNING_MEMORY * K) / BLOCK_SIZE);
		MAX_TUPLES = TUPLES_OF_BLOCK * MAX_BLOCKS;

		long startTime = System.currentTimeMillis();
		int t1SubLists = phaseOne("C:\\Users\\du_zhen\\Downloads\\demoBag1.txt", "t1");
		int t2SubLists = phaseOne("C:\\Users\\du_zhen\\Downloads\\demoBag2.txt", "t2");
		System.out.println("The number of sublists for T1 is:" + t1SubLists);
		System.out.println("The number of sublists for T2 is:" + t2SubLists);
		System.out.println("The I/O in phase1:" + iocost);
		recordTime(startTime, "The execute time for phase1");
		phaseTwo("t1", t1SubLists);
		phaseTwo("t2", t2SubLists);
		compareDiff("t1","t2");
		System.out.println("Total I/O Cost is:" + iocost + " times R/W");
		recordTime(startTime, "Total");
	}
	
	/**
	 * The input is T1 and T2 input file, the output is sorted file
	 */
	public static int phaseOne(String inputFile, String outputPrefix) {
		float startMemory = Runtime.getRuntime().freeMemory() / K;
		long startTime = System.currentTimeMillis();
		int sublistCount = 0;
		int readLength = 0;
		int lines = 0;
		if(DEBUG) {
			System.out.println("**************************"+ outputPrefix + " PHASE ONE **************************");
			System.out.println("Max tuples to fill:" + MAX_TUPLES);
		}

		byte[][] sublistbyte = new byte[MAX_TUPLES][BYTES_OF_TUPLE];
		byte[] blockBuffer = new byte[TUPLES_OF_BLOCK*BYTES_OF_TUPLE];
		if(DEBUG) {
			System.out.println("Free Memory:" + Runtime.getRuntime().freeMemory() / K + "KB");
		}
		try {
			FileInputStream ios = new FileInputStream(inputFile);
			while ((readLength = ios.read(blockBuffer))!= -1) {
				iocost++;
				for(int i=0;i<readLength/BYTES_OF_TUPLE;i++) {
					System.arraycopy(blockBuffer, BYTES_OF_TUPLE*i, sublistbyte[lines++], 0, BYTES_OF_TUPLE);
				}
				if (lines == MAX_TUPLES || readLength < blockBuffer.length) {
					// sort the sublist
					QuickSort.quicksort(sublistbyte, 0, lines - 1);
					FileOutputStream out = new FileOutputStream(outputPrefix + "_" + sublistCount++ + ".txt");
					int lineCount = 0;
					while(lineCount < lines) {
						System.arraycopy(sublistbyte[lineCount], 0, blockBuffer, BYTES_OF_TUPLE*(lineCount%TUPLES_OF_BLOCK), BYTES_OF_TUPLE);
						lineCount++;
						if(lineCount%TUPLES_OF_BLOCK == 0) {
							out.write(blockBuffer, 0, BYTES_OF_TUPLE*TUPLES_OF_BLOCK);
							iocost++;
						} else if(lineCount == lines) {
							out.write(blockBuffer, 0, BYTES_OF_TUPLE*(lineCount%TUPLES_OF_BLOCK));
							iocost++;
						}
					}
					lines = 0;
					out.close();
					out = null;
					if(DEBUG) {
						System.out.println("Finish " + sublistCount + " sublist, Free Memory:" + Runtime.getRuntime().freeMemory() / K + "KB");
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
		blockBuffer = null;
		sublistbyte = null;
		System.gc();
		return sublistCount;
	}

	/**
	 * The method is to implement the process of phase 2
	 */
	public static void phaseTwo(String outputPrefix, int sublistCount) {
		System.gc();
		float startMemory = Runtime.getRuntime().freeMemory() / K;
		long startTime = System.currentTimeMillis();
		// need one output block-size buffer
		int inputBufferBlocks = (int) Math.floor((Runtime.getRuntime().freeMemory() - RUNNING_MEMORY * K) / BLOCK_SIZE) - 1;
		if(DEBUG) {
			System.out.println("++++++++++++++++++++++++++"+ outputPrefix + " PHASE TWO ++++++++++++++++++++++++++");
			System.out.println("Max Blocks to fill:"+inputBufferBlocks);
		}
		// average blocks for every sublist
		int avgDistributedBlocks = (int) Math.floor(inputBufferBlocks / sublistCount);
		// declare output buffer
		byte[][][] inputBuffer = new byte[sublistCount][avgDistributedBlocks * TUPLES_OF_BLOCK][BYTES_OF_TUPLE];
		byte[] blockBuffer = new byte[TUPLES_OF_BLOCK*BYTES_OF_TUPLE];
		int tuplesCount[] = new int[sublistCount];
		int readLength[] = new int[sublistCount];
		FileInputStream[] mergeInput = new FileInputStream[sublistCount];
		// merge
		int outputBufferOffset = 0;
		int[] sublistIndex = new int[sublistCount];
		byte[][] outputBuffer = new byte[TUPLES_OF_BLOCK][];
		boolean fillSublist = false;
		
		try {
			for (int i = 0; i < sublistCount; i++) {
				mergeInput[i] = new FileInputStream(outputPrefix + "_" + i + ".txt");
			}
			boolean finish=false;
			int phase2FillCount = 0;
			FileOutputStream out = new FileOutputStream(outputPrefix + "_sorted.txt");
			while(!finish) {
				//first loading
				for (int i = 0; i < sublistCount; i++) {
					if(tuplesCount[i] != 0 || readLength[i] == -1) {
						continue;
					}
					int tuples = 0;
					while ((readLength[i] = mergeInput[i].read(blockBuffer))!= -1) {
						iocost++;
						for(int t=0;t<readLength[i]/BYTES_OF_TUPLE;t++) {
							System.arraycopy(blockBuffer, BYTES_OF_TUPLE*t, inputBuffer[i][tuples++], 0, BYTES_OF_TUPLE);
						}
						tuplesCount[i] = tuples;
						if (tuples / TUPLES_OF_BLOCK == avgDistributedBlocks) {
							break;
						}
					}
				}
				if(DEBUG) {
					recordTime(startTime, outputPrefix + " phase 2-"+phase2FillCount+" time fill");
					recordMemory(startMemory, outputPrefix + " phase 2-"+phase2FillCount+" time fill");
				}
				//Multi-way Merge
				int thisSublist = 0;
				fillSublist = false;
				while (!fillSublist) {
//					System.out.println(MAX_ID.length);
//					!!System.arraycopy(MAX_ID, 0, outputBuffer[outputBufferOffset], 0, TUPLEBYTE);
					outputBuffer[outputBufferOffset] = MAX_ID;
					for (int j = 0; j < sublistCount; j++) {
						if (sublistIndex[j] < tuplesCount[j] && compare(inputBuffer[j][sublistIndex[j]], outputBuffer[outputBufferOffset]) < 0) {
//							System.out.println(sublistIndex[j] + "," + outputBufferOffset);
//							!!System.arraycopy(inputBuffer[j][sublistIndex[j]], 0, outputBuffer[outputBufferOffset], 0, TUPLEBYTE);
							outputBuffer[outputBufferOffset] = inputBuffer[j][sublistIndex[j]];
							thisSublist = j;
						}
//						for (int i = 0; i < tuplesCount[j]; i++) {
//							out.write(inputBuffer[j][i], 0, TUPLEBYTE - 1);
//							out.write('\n');
//						}
					}
					if(compare(outputBuffer[outputBufferOffset], MAX_ID) == 0) {
						finish = true;
					} else {
						outputBufferOffset++;
						sublistIndex[thisSublist]++;
						if(sublistIndex[thisSublist] == tuplesCount[thisSublist]) {
							if(readLength[thisSublist] != -1) {
								sublistIndex[thisSublist] = 0;
								tuplesCount[thisSublist] = 0;
								fillSublist = true;							
							}
						}
					}
					//Be careful, outputBuffer store the reference of inputBuffer, save to file before overwrite the inputBuffer
					if (outputBufferOffset == TUPLES_OF_BLOCK || fillSublist || finish) {
						// write back
						for (int i = 0; i < outputBufferOffset; i++) {
							System.arraycopy(outputBuffer[i], 0, blockBuffer, BYTES_OF_TUPLE*i, BYTES_OF_TUPLE);
						}
						out.write(blockBuffer, 0, BYTES_OF_TUPLE*outputBufferOffset);
						iocost++;
						outputBufferOffset = 0;
					}
					if(finish) {
						break;
					}
				}
				phase2FillCount++;
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
		blockBuffer = null;
		tuplesCount = null;
		readLength = null;
		mergeInput = null;
		//merge
		sublistIndex = null;
		outputBuffer = null;
		System.gc();
	}

	/**
	 * compare the diff between two sorted relations
	 */
	public static void compareDiff(String t1Prefix, String t2Prefix){
		System.gc();
		float runtimeMemoryParam = 0.5f;   // less than 1
		float startMemory = Runtime.getRuntime().freeMemory() / K;
		long startTime = System.currentTimeMillis();

		byte[] outputBuffer = new byte[TUPLES_OF_BLOCK * BYTES_OF_TUPLE];
		byte[] blockBuffer = new byte[TUPLES_OF_BLOCK * BYTES_OF_TUPLE];

		int inputBufferBlocks = (int) Math.floor((Runtime.getRuntime().freeMemory() - RUNNING_MEMORY * K) / BLOCK_SIZE * runtimeMemoryParam);

		if(DEBUG) {
			System.out.println("=========================== COMPARE DIFF ===========================");
			System.out.println("Max Blocks to fill:"+inputBufferBlocks);
		}

		try {
			FileInputStream ios1 = new FileInputStream(t1Prefix + "_sorted.txt");
			FileInputStream ios2 = new FileInputStream(t2Prefix + "_sorted.txt");
			FileOutputStream out = new FileOutputStream("result.txt");

			int avgDistributedBlocks = (int) Math.floor(inputBufferBlocks / 2);
			//int avgDistributedBlocks = 100;

			if(DEBUG){
				System.out.println("Avg Distributed blocks:"+avgDistributedBlocks);
			}
			//t1
			byte[][] t1Buffer = new byte[avgDistributedBlocks * TUPLES_OF_BLOCK][BYTES_OF_TUPLE];
			//t2
			byte[][] t2Buffer = new byte[avgDistributedBlocks * TUPLES_OF_BLOCK][BYTES_OF_TUPLE];

			//record how much tuple in memory buffer
			int lenOft1Buffer;
			int lenOft2Buffer;

			//first load
			lenOft1Buffer = loadFile(t1Buffer,blockBuffer,ios1);
			lenOft2Buffer = loadFile(t2Buffer,blockBuffer,ios2);

			//compare
			int currentT1 = 0;	   //pointer
			int currentT2 = 0;
			int outputPointer = 0;

			while (true){
				if(compare(t1Buffer[currentT1], t2Buffer[currentT2]) > 0){
					byte[] tempT2 = t2Buffer[currentT2].clone();

					while (compare(tempT2,t2Buffer[currentT2]) == 0){
						currentT2++;

						if(currentT2 == lenOft2Buffer){
							lenOft2Buffer = loadFile(t2Buffer,blockBuffer,ios2);
							currentT2 = 0;

							//trick
							if(lenOft2Buffer < t2Buffer.length){
								byte[] biggest = new byte[101];
								Arrays.fill(biggest,Byte.MAX_VALUE);
								t2Buffer[lenOft2Buffer] = biggest;
								lenOft2Buffer += 1;
							}
						}
					}

					outputPointer = writeFile(outputBuffer,outputPointer,tempT2,0,out);

				}
				else if(compare(t1Buffer[currentT1], t2Buffer[currentT2]) < 0){
					byte[] tempT1 = t1Buffer[currentT1].clone();
					int counter = 0;

					while (compare(tempT1,t1Buffer[currentT1]) == 0){
						counter++;
						currentT1++;

						if(currentT1 == lenOft1Buffer){
							lenOft1Buffer =loadFile(t1Buffer,blockBuffer,ios1);
							currentT1 = 0;

							//trick
							if(lenOft1Buffer < t1Buffer.length){		//end of file
								byte[] biggest = new byte[100];
								Arrays.fill(biggest,Byte.MAX_VALUE);
								t1Buffer[lenOft1Buffer] = biggest;
								lenOft1Buffer += 1;
							}
						}
					}

					outputPointer = writeFile(outputBuffer,outputPointer,tempT1,counter,out);

				}
				else {	//equal
					if(isMaxByte(t1Buffer[currentT1])){
						System.out.println("=========================== END OF COMPARE DIFF ===========================");

						//write final output buffer to file
						out.write(outputBuffer,0,outputPointer);
						iocost++;
						break;
					}

					byte[] tempT = t1Buffer[currentT1].clone();

					int counter1 = 0;
					while (compare(tempT,t1Buffer[currentT1]) == 0){
						counter1++;
						currentT1++;

						if(currentT1 == lenOft1Buffer){
							lenOft1Buffer = loadFile(t1Buffer,blockBuffer,ios1);
							currentT1 = 0;

							//trick
							if(lenOft1Buffer < t1Buffer.length){		//end of file
								byte[] biggest = new byte[101];
								Arrays.fill(biggest,Byte.MAX_VALUE);
								t1Buffer[lenOft1Buffer] = biggest;
								lenOft1Buffer += 1;
							}
						}
					}


					int counter2 = 0;
					while (compare(tempT,t2Buffer[currentT2]) == 0){
						counter2++;
						currentT2++;

						if(currentT2 == lenOft2Buffer){
							lenOft2Buffer = loadFile(t2Buffer,blockBuffer,ios2);
							currentT2 = 0;

							//trick
							if(lenOft2Buffer < t2Buffer.length){
								byte[] biggest = new byte[101];
								Arrays.fill(biggest,Byte.MAX_VALUE);
								t2Buffer[lenOft2Buffer] = biggest;
								lenOft2Buffer += 1;
							}
						}
					}

					if(counter1 > counter2){
						outputPointer = writeFile(outputBuffer,outputPointer,tempT,counter1-counter2,out);
					}else{
						outputPointer = writeFile(outputBuffer,outputPointer,tempT,0,out);
					}


					tempT = null;
					counter1 = 0;
					counter2 = 0;
				}
			}

			ios1.close();
			ios2.close();
			out.close();

		}catch (IOException e){
			System.out.println(e.getMessage());
		}

		if(DEBUG) {
			recordTime(startTime, "compare diff time fill");
			recordMemory(startMemory, "compare diff time fill");
		}
	}

	public static void recordTime(long startTime, int divide, String function) {
		long endTime = System.currentTimeMillis();
		long duration = (endTime - startTime);
		System.out.println(function + " time is:" + duration/divide + " milliseconds");
	}
	
	public static void recordTime(long startTime, String function) {
		recordTime(startTime, 1, function);
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


	public static boolean isMaxByte(byte[] byt){
		boolean flag = true;
		for(byte b:byt){
			if(b != Byte.MAX_VALUE){
				flag = false;
				break;
			}
		}
		return flag;
	}



	/**
	 *  return the length of tuple in memory buffer, return 0 if end of file
	 */
	public static int loadFile(byte[][] buffer, byte[] blockBuffer, FileInputStream ios) throws IOException {
		int readLength = 0;
		int i = 0;
		int lineOfBuffer = 0;

		while (i < buffer.length){    //buffer.length = max length of tuple in buffer
			readLength = ios.read(blockBuffer);
			if(readLength == -1){
				break;
			}
			iocost++;
			i += (readLength / BYTES_OF_TUPLE);

			for(int t=0; t<readLength/BYTES_OF_TUPLE; t++) {
				System.arraycopy(blockBuffer, BYTES_OF_TUPLE*t, buffer[lineOfBuffer++], 0, BYTES_OF_TUPLE);
			}
		}

		return i;
	}


	public static int writeFile(byte[] outputBuffer, int outputPointer, byte[] tuple, int writeNum, FileOutputStream out) {

		byte[] numberByte = String.valueOf(writeNum).getBytes();
		if (outputBuffer.length - outputPointer - 1 < numberByte.length + extraLength + BYTES_OF_TUPLE) {        //save to file
			try {
				out.write(outputBuffer, 0, outputPointer);
				iocost++;
				outputPointer = 0;
			} catch (IOException e) {
				System.out.println(e.getMessage());
			}
		}

		//copy data
		System.arraycopy(extra1, 0, outputBuffer, outputPointer, extra1.length);
		outputPointer += extra1.length;

		System.arraycopy(tuple, 0, outputBuffer, outputPointer, BYTES_OF_TUPLE-1);
		outputPointer += (BYTES_OF_TUPLE-1);

		System.arraycopy(extra2, 0, outputBuffer, outputPointer, extra2.length);
		outputPointer += extra2.length;

		System.arraycopy(numberByte, 0, outputBuffer, outputPointer, numberByte.length);
		outputPointer += numberByte.length;

		System.arraycopy(extra3, 0, outputBuffer, outputPointer, extra3.length);
		outputPointer += extra3.length;

		return outputPointer;
	}


}
