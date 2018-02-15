import java.io.*;
import java.util.Arrays;
import java.util.Comparator;

public class RelationDiff {

    public static void main(String[] args) {

        System.out.println("Total Memory:" + Runtime.getRuntime().totalMemory() / (1024 * 1024) + "MB");
        System.out.println("Free Memory:" + Runtime.getRuntime().freeMemory() / (1024 * 1024) + "MB");

        int expectedBlocksInMemory = (int) Math.floor(Runtime.getRuntime().freeMemory() / (4 * 1024));

        File file = new File("");

        if (file.exists()) {

            int maxNumOfTuple = 40 * expectedBlocksInMemory;
            String[][] sublist;
            boolean flag = true;

            while (flag) {
                sublist = new String[maxNumOfTuple][7];

                try {
                    BufferedReader bufferedReader = new BufferedReader(new FileReader(file));

                    int i = 0;
                    for (; i < maxNumOfTuple; i++) {
                        String line = bufferedReader.readLine();
                        if (line == null) {
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

                    bufferedReader.close();


                    //sort
                    Arrays.sort(sublist, 0, i, new Comparator<String[]>() {   //todo:i-1
                        @Override
                        public int compare(String[] tuple1, String[] tuple2) {
                            return tuple1[0].compareTo(tuple2[0]);
                        }
                    });


                    //write sublist into file
                    int sublistCount = 0;

                    String outputFileName = "sublist" + sublistCount + ".txt";
                    BufferedWriter out = new BufferedWriter(new FileWriter(outputFileName));
                    int lineCount = 0;
                    for (int n = 0; n<i; n++) {
                        out.append((CharSequence) (sublist[i][0]+sublist[i][2]+ sublist[i][3]+sublist[i][4]+sublist[i][5]+ sublist[i][6]));
                        lineCount ++;
                        if(lineCount == 39){
                            lineCount = 0;
                            out.newLine();
                        }
                    }
                    out.close();

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }


    }
}
