
public class BestPerformance {

	public static final int TIMES = 10;
	public static void testBest() {
		float freeMemory = Runtime.getRuntime().freeMemory() / (RelationDiff.K * RelationDiff.K);
		System.out.println("Free Memory:" + freeMemory + "MB");
		float start = 3.0f;
		float add = 0.1f;
		if(freeMemory > 6) {
			start = 3.0f;
			add = 0.5f;
		} else {
			start = 1.6f;
			add = 0.1f;
		}
		for(float i=start;i<freeMemory;i+=add) {
			RelationDiff.RUNNING_MEMORY = 1024;//k
			RelationDiff.RUNNING_MEMORY = i*RelationDiff.RUNNING_MEMORY;
			RelationDiff.MAX_BLOCKS = (int) Math.floor((Runtime.getRuntime().freeMemory() - RelationDiff.RUNNING_MEMORY * RelationDiff.K) / RelationDiff.BLOCK_SIZE);
			RelationDiff.MAX_TUPLES = RelationDiff.TUPLES_OF_BLOCK * RelationDiff.MAX_BLOCKS;
			
			System.out.println("Max tuples to fill:" + RelationDiff.MAX_TUPLES);
			long startTime = System.currentTimeMillis();
			int t1SubLists = 0;
			for(int j=0;j<TIMES;j++) {
				t1SubLists = RelationDiff.phaseOne("E:\\bag1.txt", "t1");
				RelationDiff.phaseTwo("t1", t1SubLists);
			}
			freeMemory = Runtime.getRuntime().freeMemory() / (RelationDiff.K * RelationDiff.K);
			RelationDiff.recordTime(startTime, TIMES, "i="+i+",sublists:"+t1SubLists+",Everage");
		}
	}
	
	public static void main(String[] args) {
		testBest();
	}

}
