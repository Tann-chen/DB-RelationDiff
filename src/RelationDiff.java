public class RelationDiff {

    public static Long totalMemorySize;


    public static void main(String[] args){
        //total available memory
        System.out.println("Total Memory:" + Runtime.getRuntime().freeMemory() / (1024*1024) + "MB");



    }
}
