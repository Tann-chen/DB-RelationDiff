# DB-RelationDiff
Java-based project to compare diff between relations, using limited memory

## Command Line
```
cd src
javac RelationDiffByte.java QuickSortTupleByte.java
java -Xmx5M RelationDiffByte
```
## Jar Running
```
jar cvfe RelationDiff.jar RelationDiffByte *.class
java -Xmx5M -jar RelationDiff
```