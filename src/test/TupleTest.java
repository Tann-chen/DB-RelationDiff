package test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Random;

public class TupleTest {

	public TupleTest() {
		String[][] tuples = new String[50000][7];
		Random r = new Random();
		for (int i = 0; i < tuples.length; i++) {
			String[] item = { String.valueOf(r.nextInt(99999999)), "Hello", "World", "CS", "COMP6521", "6521",
					"Concordia" };
			tuples[i] = item;
		}
		QuickSortTuples.quicksort(tuples, 0, tuples.length - 1);
		for (int i = 0; i < tuples.length; i++) {
			// System.out.println(tuples[i][0]);
		}
		// System.out.println(tuples.length);
	}

	public static void recordTime(long startTime, String function) {
		long endTime = System.currentTimeMillis();
		long duration = (endTime - startTime);
		System.out.println(function + " duration time:" + duration + " milliseconds");
	}

	public static void recordMemory(long start, String funciont) {
//		System.gc();
		long used = start - Runtime.getRuntime().freeMemory() / (1024);
		System.out.println("Memory used:" + used + "KB");
		System.out.println("Total Memory:" + Runtime.getRuntime().totalMemory() / (1024 * 1024.0) + "MB");
        System.out.println("Free Memory:" + Runtime.getRuntime().freeMemory() / (1024 * 1024.0) + "MB\n");
	}

	private static void testIntegerArray() {
		Random r = new Random();
		long startMemory = Runtime.getRuntime().freeMemory() / (1024);
		long startTime = System.currentTimeMillis();
		int[] quick = new int[50000];
		for (int i = 0; i < quick.length; i++) {
			quick[i] = r.nextInt(49999999);
		}
		QuickSort.quicksort(quick, 0, quick.length - 1);
//		for (int i = 0; i < quick.length; i++) {
//			System.out.println(quick[i]);
//		}
		recordTime(startTime, "Integer array");
		recordMemory(startMemory, "Integer array");
	}

	private static void testObjectArrayList() {
		Random r = new Random();
		long startMemory = Runtime.getRuntime().freeMemory() / (1024);
		long startTime = System.currentTimeMillis();
		
		ArrayList<Tuple> tuplesAL = new ArrayList<Tuple>();
		
		for (int i = 0; i < 50000; i++) {
			Tuple t = new Tuple(r.nextInt(49999999), new String("Gregory   "), new String("Norris    "), 123, 521, 772150098,
					new String("772150098Roiso Point,Cockanhow,AB,X2G 3B2                         "));
			tuplesAL.add(t);
		}
		tuplesAL.sort(new Comparator<Tuple>( ) {
			@Override
		    public int compare(Tuple o1, Tuple o2) {
		        return o1.id - o2.id;
		    }
		});
//		for (Tuple t : tuplesAL) {
//			System.out.println(t.id);
//		}
		
		recordTime(startTime, "Object arraylist");
		recordMemory(startMemory, "Object arraylist");
	}

	private static void testObjectArray() {
		Random r = new Random();
		long startMemory = Runtime.getRuntime().freeMemory() / (1024);
		long startTime = System.currentTimeMillis();
		
		Tuple[] tuplesO = new Tuple[50000];
		r = new Random();
		for (int i = 0; i < 50000; i++) {
			Tuple t = new Tuple(r.nextInt(49999999), new String("Gregory   "), new String("Norris    "), 123, 521, 772150098,
					new String("772150098Roiso Point,Cockanhow,AB,X2G 3B2                         "));
			tuplesO[i] = t;
		}
		QuickSortTuplesObject.quicksort(tuplesO, 0, tuplesO.length - 1);
//		for (int i = 0; i < tuplesO.length; i++) {
//			System.out.println(tuplesO[i].id);
//		}
		
		recordTime(startTime, "Object array");
		recordMemory(startMemory, "Object array");
	}
	
	private static void testObjectByteArray() {
		Random r = new Random();
		long startMemory = Runtime.getRuntime().freeMemory() / (1024);
		long startTime = System.currentTimeMillis();
		
		TupleByte[] tuplesB = new TupleByte[50000];
		r = new Random();
		for (int i = 0; i < 50000; i++) {
			TupleByte t = new TupleByte(r.nextInt(49999999), new String("Gregory   ").getBytes(), new String("Norris    ").getBytes(), 123, 521, 772150098,
					new String("772150098Roiso Point,Cockanhow,AB,X2G 3B2                         ").getBytes());
			tuplesB[i] = t;
		}
//		QuickSortTuplesObject.quicksort(TupleByte, 0, TupleByte.length - 1);
//		for (int i = 0; i < tuplesO.length; i++) {
//			System.out.println(tuplesO[i].id);
//		}
		
		recordTime(startTime, "Object array");
		recordMemory(startMemory, "Object array");
	}

	private static void test100Byte() {
		Random r = new Random();
		long startMemory = Runtime.getRuntime().freeMemory() / (1024);
		long startTime = System.currentTimeMillis();
		
//		Tuple[] tuplesO = new Tuple[50000];
		byte[][] tuples = new byte[50000][100];
		r = new Random();
		for (int i = 0; i < 50000; i++) {
//			String byte100 = "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890";
//			char byte100[] = ("12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345"+r.nextInt(50000)).toCharArray();
//			tuples[i] = byte100;
			for(int j = 0;j<100;j++) {
				tuples[i][j] = 'a';
			}
			//			Tuple t = new Tuple(r.nextInt(49999999), "Gregory   ", "Norris    ", 123, 521, 772150098,
//					"772150098Roiso Point,Cockanhow,AB,X2G 3B2                         ");
//			tuplesO[i] = t;
		}
//		QuickSortTuplesObject.quicksort(tuplesO, 0, tuplesO.length - 1);
//		for (int i = 0; i < tuplesO.length; i++) {
//			System.out.println(tuplesO[i].id);
//		}
		
//		recordTime(startTime, "100Byte array");
		recordMemory(startMemory, "100Byte array");
	}
	
	private static void test100String() {
		Random r = new Random();
		long startMemory = Runtime.getRuntime().freeMemory() / (1024);
		long startTime = System.currentTimeMillis();
		
//		Tuple[] tuplesO = new Tuple[50000];
		String[] tuples = new String[50000];
		r = new Random();
		for (int i = 0; i < 50000; i++) {
//			String byte100 = "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890";
			String byte100 = new String("12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345"+r.nextInt(50000));
			tuples[i] = byte100;
			//			Tuple t = new Tuple(r.nextInt(49999999), "Gregory   ", "Norris    ", 123, 521, 772150098,
//					"772150098Roiso Point,Cockanhow,AB,X2G 3B2                         ");
//			tuplesO[i] = t;
		}
//		QuickSortTuplesObject.quicksort(tuplesO, 0, tuplesO.length - 1);
//		for (int i = 0; i < tuplesO.length; i++) {
//			System.out.println(tuplesO[i].id);
//		}
		
//		recordTime(startTime, "100Byte array");
		recordMemory(startMemory, "100Byte array");
	}
	
	private static void testStringArray() {
		Random r = new Random();
		long startMemory = Runtime.getRuntime().freeMemory() / (1024);
		long startTime = System.currentTimeMillis();
		String[][] tuples = new String[50000][7];
		for (int i = 0; i < tuples.length; i++) {
			tuples[i] = null;
			tuples[i] = new String[]{ ""+r.nextInt(49999999), new String("Gregory   "), new String("Norris    "), new String("123"), new String("521"), new String("772150098"),
					new String("772150098Roiso Point,Cockanhow,AB,X2G 3B2                         ") };
		}
		startTime = System.currentTimeMillis();
//		QuickSortTuplesString.quicksort(tuples, 0, tuples.length - 1);
//		for (int i = 0; i < tuples.length; i++) {
//			System.out.println(tuples[i][0]);
//		}
		recordTime(startTime, "Primitive string array");
		recordMemory(startMemory, "Primitive string array");
	}
	
	
	public static void main(String[] args) {
		System.out.println("Total Memory:" + Runtime.getRuntime().freeMemory() / (1024 * 1024.0) + "MB");
        
//		test100Byte();
		
//		test100String();
		
//		testStringArray();
		
		testObjectArray();

//		testObjectByteArray();
		
//		testObjectArrayList();
//
//		testIntegerArray();
		
		System.out.println("Free Memory:" + Runtime.getRuntime().freeMemory() / (1024 * 1024.0) + "MB");
	}
}
