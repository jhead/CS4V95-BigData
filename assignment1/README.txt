Authors:
- Justin Head, jxh122430
- Fannyu Chien, ffc140030

== Project Compilation ==
Run "mvn install", it will create two useful jars under "target/"
- jxh122430-assignment1-part1.jar
- jxh122430-assignment1-part2.jar

== Part 1 ==
Usage: java -jar jxh122430-assignment1-part1.jar <base-dir>

Base dir dictates the output directory within HDFS. In my case, I used "/user/jxh122430" as my paramter.

== Part 2 ==
Usage: java -jar jxh122430-assignment1-part2.jar <base-dir>

(same as above)

Corpus: Wikipedia (linear text)
Corpus URL: http://corpus.byu.edu/wikitext-samples/text.zip

Corpus zip file is downloaded and decompressed automatically into HDFS via ZipInputStream.

Note: it may display a "Stream closed" exception at the end - this is normal
