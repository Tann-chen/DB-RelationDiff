# DB-RelationDiff
Java-based project to compare diff between relations, using limited memory

## Command Line
```
cd src
javac RelationDiff.java QuickSort.java
java -Xmx5M RelationDiff
```
## Jar Running
```
jar cvfe RelationDiff.jar RelationDiff *.class
java -Xmx5M -jar RelationDiff
```
## Reference
[QuickSort](https://anh.cs.luc.edu/363/handouts/Quicksort.java)