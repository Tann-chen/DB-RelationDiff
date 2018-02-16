import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

public class RelationDiff {

    public static int expectedBlocksInMemory;

    public static void main(String[] args) {
        System.out.println("Total Memory:" + Runtime.getRuntime().totalMemory() / (1024 * 1024) + "MB");
        System.out.println("Free Memory:" + Runtime.getRuntime().freeMemory() / (1024 * 1024) + "MB");

        expectedBlocksInMemory = (int) Math.floor(Runtime.getRuntime().freeMemory() / (4 * 1024));

        File t1 = new File("");  //todo:inputFileName
        File t2 = new File("");
        int t1SubLists = phaseOne(t1, "t1");
        int t2SubLists = phaseOne(t2, "t2");


    }


    /**
     * The input is T1 and T2 input file, the output is sorted file
     */
    public static int phaseOne(File inputFile, String outputPrefix) {
        int sublistCount = 0;

        if (inputFile.exists()) {
            int maxNumOfTuple = 40 * expectedBlocksInMemory;  //except the last block
            String[][] sublist;
            boolean flag = true;

            while (flag) {
                sublist = new String[maxNumOfTuple][7];

                try {
                    BufferedReader in = new BufferedReader(new FileReader(inputFile));

                    int i = 0;
                    for (; i < maxNumOfTuple; i++) {
                        String line = in.readLine();      //every line is a tuple in input file
                        if (line == null) {        //end of the input file
                            flag = false;
                            break;
                        }

                        sublist[i][0] = line.substring(0, 8);
                        sublist[i][1] = line.substring(8, 18);
                        sublist[i][2] = line.substring(18, 28);
                        sublist[i][3] = line.substring(28, 31);
                        sublist[i][4] = line.substring(31, 34);
                        sublist[i][5] = line.substring(34, 43);
                        sublist[i][6] = line.substring(43, 100);
                    }

                    in.close();


                    //sort the sublist
                    Arrays.sort(sublist, 0, i, new Comparator<String[]>() {   //todo:i-1
                        @Override
                        public int compare(String[] tuple1, String[] tuple2) {
                            return tuple1[0].compareTo(tuple2[0]);
                        }
                    });


                    //write sublist into file
                    String outputFileName = outputPrefix + "_" + sublistCount + ".txt";
                    BufferedWriter out = new BufferedWriter(new FileWriter(outputFileName));
                    int lineCount = 0;
                    for (int j = 0; j < i; j++) {
                        out.append((CharSequence) (sublist[j][0] + sublist[j][1] + sublist[j][2] + sublist[j][3] + sublist[j][4] + sublist[j][5] + sublist[j][6]));
                        lineCount++;
                        if (lineCount == 40) {
                            lineCount = 0;
                            out.newLine();
                        }
                    }
                    out.close();

                    sublistCount++;

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }

        return sublistCount;

    }


    /**
     * The method is to implement the process of phase 2
     */
    public static void phaseTwo(String outputPrefix, int sublistCount) {
        //need one output block-size buffer
        int inputBufferBlocks = (int) Runtime.getRuntime().freeMemory() / (4 * 1024) - 1;

        //declare output buffer
        String[][] outputBuffer = new String[40][7];
        int outputBufferCurrentIndex = 0;

        //average blocks for every sublist
        int avgDistributedBlocks = (int) Math.floor(inputBufferBlocks / sublistCount);

        //String[][][] sublists = new String[sublistCount][avgDistributedBlocks * 40][7];
        ArrayList<String[]>[] sublists = new ArrayList[sublistCount];

        for (int i = 0; i < sublistCount; i++) {

            sublists[i] = new ArrayList<>();

            try {
                BufferedReader in = new BufferedReader(new FileReader(outputPrefix + "_" + i + ".txt"));
                for (int b = 0; b < avgDistributedBlocks; b++) {
                    String tempBlock = in.readLine();   //one block per line
                    int pointer = 0;  //point to current char;
                    int currentTuple = 0;
                    while (tempBlock.charAt(pointer) != '\r') {
                        String tempTuple = tempBlock.substring(pointer, pointer + 100);
//                        sublists[i][b * 40 + currentTuple][0] = tempTuple.substring(0, 8);
//                        sublists[i][b * 40 + currentTuple][1] = tempTuple.substring(8, 18);
//                        sublists[i][b * 40 + currentTuple][2] = tempTuple.substring(18, 28);
//                        sublists[i][b * 40 + currentTuple][3] = tempTuple.substring(28, 31);
//                        sublists[i][b * 40 + currentTuple][4] = tempTuple.substring(31, 34);
//                        sublists[i][b * 40 + currentTuple][5] = tempTuple.substring(34, 43);
//                        sublists[i][b * 40 + currentTuple][6] = tempTuple.substring(43, 100);
                        String [] temp = new String[7];
                        temp[0] = tempTuple.substring(0, 8);
                        temp[1] = tempTuple.substring(8, 18);
                        temp[2] = tempTuple.substring(18, 28);
                        temp[3] = tempTuple.substring(28, 31);
                        temp[4] = tempTuple.substring(31, 34);
                        temp[5] = tempTuple.substring(34, 43);
                        temp[6] = tempTuple.substring(43, 100);
                        sublists[i].add(temp);

                        pointer += 100;
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();
            }

        }



        //merging
        int[] currentPoints = new int[sublistCount];
        int exhaustedFlag = -1;

        while (exhaustedFlag < 0){

            // set temp is current tuple id in first sublist
            String temp = sublists[0].get(currentPoints[0])[0];
            int tempIndex = 0;

            //compare to find smallest
            for(int j=1; j<sublistCount; j++){
                if(sublists[j].get(currentPoints[j])[0].compareTo(temp) < 0){
                    temp = sublists[j].get(currentPoints[j])[0];
                    tempIndex = j;
                }
            }

            //write the smallest into output buffer
            outputBuffer[outputBufferCurrentIndex][0] = sublists[tempIndex].get(currentPoints[tempIndex])[0];
            outputBuffer[outputBufferCurrentIndex][1] = sublists[tempIndex].get(currentPoints[tempIndex])[1];
            outputBuffer[outputBufferCurrentIndex][2] = sublists[tempIndex].get(currentPoints[tempIndex])[2];
            outputBuffer[outputBufferCurrentIndex][3] = sublists[tempIndex].get(currentPoints[tempIndex])[3];
            outputBuffer[outputBufferCurrentIndex][4] = sublists[tempIndex].get(currentPoints[tempIndex])[4];
            outputBuffer[outputBufferCurrentIndex][5] = sublists[tempIndex].get(currentPoints[tempIndex])[5];
            outputBuffer[outputBufferCurrentIndex][6] = sublists[tempIndex].get(currentPoints[tempIndex])[6];

            // next time write tuple to next index in output buffer
            outputBufferCurrentIndex ++;

            //current point of selected sublist in this run should shift
            currentPoints[tempIndex] ++;

            if(outputBufferCurrentIndex >= 40){
                //todo:write output buffer to file


                outputBufferCurrentIndex = 0;
            }

            if(currentPoints[tempIndex] == sublists[tempIndex].size()){
                //todo:read one block of this into memory

            }
        }


    }


}
