import java.io.FileInputStream;

import org.junit.jupiter.api.Test;

class RelationDiffTest {

	private static void testFile(String file) {

		try {
			int eof = 0;
			int lines = 0;
			byte[] pre = new byte[101];
			byte[] buffer = new byte[101];
			FileInputStream ios = new FileInputStream(file);
			eof = ios.read(pre);
			while (eof != -1) {
				eof = ios.read(buffer);
				lines++;
				if (RelationDiff.compare(pre, buffer) > 0) {
					System.out.println("Wrong order lien:" + lines);
					assert (RelationDiff.compare(pre, buffer) < 0);
				}
				System.arraycopy(buffer, 0, pre, 0, 101);

			}
			ios.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	void test() {
		testFile("t1_sorted.txt");
	}

	@Test
	void test2() {
		testFile("t2_sorted.txt");
	}

}
