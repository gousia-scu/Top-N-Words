The Map Reduce program is implemented as two programs.
1)TopNWords
2)TopNWordsGreaterThanSixChars
All the dependednt jar files are added in the libs folder
To the run the code
1) Make a jar file of the code
2) Commands to execute
hadoop jar TopNWords.jar TopNWords.TopWords <input file> <output file>
hadoop jar TopNWordsGreaterThanSixChars.jar TopNWords.TopWords <input file> <output file>

Hive
The hive program has the name of the file embeded to the code
to execute the script(assuming both code and data is in the same folder)
hive -f <wc.hql>