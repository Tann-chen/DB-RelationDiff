import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class RelationDiffByte {

	public static int maxNumOfTuple;
	public static int expectedBlocksInMemory;
	public static final float K = 1024.0f;
	public static final int TUPLESPB = 40;
	public static final int TUPLEBYTE = 101;
	public static final int BLOCKSIZE = TUPLESPB * TUPLEBYTE;// one block has 40 tuples, 101byte for one tuple
	public static final int RUNNINGMEMORY = 1290; // kb
	public static final int RUNNINGMEMORY_PHRASE2 = RUNNINGMEMORY+40;

	public static void main(String[] args) {
		System.out.println("Total Memory:" + Runtime.getRuntime().totalMemory() / (K * K) + "MB");
		System.out.println("Free Memory:" + Runtime.getRuntime().freeMemory() / (K * K) + "MB");

		expectedBlocksInMemory = (int) Math.floor((Runtime.getRuntime().freeMemory() - RUNNINGMEMORY * K) / BLOCKSIZE);
		maxNumOfTuple = TUPLESPB * expectedBlocksInMemory;
		String file1 = "";
		String file2 = "";
		if (args.length > 1) {
			file1 = args[0];
			file2 = args[1];
		}
		int t1SubLists = phaseOne(file1, "t1");
		phaseTwo("t1", t1SubLists);
		
		int t2SubLists = phaseOne(file2, "t2");
		phaseTwo("t2", t2SubLists);
	}

	/**
	 * The input is T1 and T2 input file, the output is sorted file
	 */
	public static int phaseOne(String inputFile, String outputPrefix) {
		System.gc();
		float startMemory = Runtime.getRuntime().freeMemory() / K;
		long startTime = System.currentTimeMillis();
		int sublistCount = 0;
		int eof = 0;
		int lines = 0;
		System.out.println("Max tuple to fill:"+maxNumOfTuple);

		byte[][] sublistbyte = new byte[maxNumOfTuple][TUPLEBYTE];
		System.out.println("Free Memory:" + Runtime.getRuntime().freeMemory() / K + "KB");
		try {
			FileInputStream ios = new FileInputStream(inputFile);
			while (eof != -1) {
				eof = ios.read(sublistbyte[lines++]);
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
						lineCount++;
						if (lineCount == TUPLESPB) {
							lineCount = 0;
							out.write('\n');
						}
					}
					lines = 0;
					out.close();
					System.out.println("Free Memory:" + Runtime.getRuntime().freeMemory() / K + "KB");
				}
			}
			ios.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		recordTime(startTime, "phase 1");
		recordMemory(startMemory, "phase 1");
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
		int inputBufferBlocks = (int) Math.floor((Runtime.getRuntime().freeMemory() - (RUNNINGMEMORY_PHRASE2) * K) / BLOCKSIZE)
				- 1;
		System.out.println("Max Blocks to fill:"+inputBufferBlocks);
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
			int phrase2_step = 0;
			FileOutputStream out = new FileOutputStream(outputPrefix + "_sorted.txt");
			while(!finish) {
				tuplesCount = new int[sublistCount];
				for (int i = 0; i < sublistCount; i++) {
					int tuples = 0;
					while (sublistEof[i] != -1) {
						sublistEof[i] = mergeInput[i].read(inputBuffer[i][tuples++]);
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
				
				recordTime(startTime, "phase 2-"+phrase2_step+" time fill");
				recordMemory(startMemory, "phase 2-"+phrase2_step+" time fill");
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
						if (sublistIndex[j] < tuplesCount[j]
								&& compare(inputBuffer[j][sublistIndex[j]], outputBuffer[outputBufferOffset]) < 0) {
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
						}
						outputBufferOffset = 0;
					}
				}
				finish = true;
				for (int i = 0; i < sublistCount; i++) {
					if(sublistEof[i] != -1) {
						finish = false;
						break;
					}
				}
				phrase2_step++;
			}
			out.close();
			for (int i = 0; i < sublistCount; i++) {
				mergeInput[i].close();;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void recordTime(long startTime, String function) {
		long endTime = System.currentTimeMillis();
		long duration = (endTime - startTime);
		System.out.println(function + " duration time:" + duration + " milliseconds");
	}

	public static void recordMemory(float start, String funciont) {
		float used = start - Runtime.getRuntime().freeMemory() / K;
		System.out.println("Memory used:" + used + "KB");
		System.out.println("Total Memory:" + Runtime.getRuntime().totalMemory() / (K * K) + "MB");
		System.out.println("Free Memory:" + Runtime.getRuntime().freeMemory() / (K * K) + "MB\n");
	}
}
