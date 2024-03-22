Databases Coursework

DBCoursework.java can be run in different ways, buy all work properly.

It requires the usage of the sqlite and slf4j-api jar files
The versions used to develop this project were:
- sqlite-jdbc - 3.45.1.0
- slf4j-api - 1.7.36


It can be run with the commands:
- javac DBCoursework.java
- java -cp ".;sqlite-jdbc-3.45.1.0.jar;slf4j-api-1.7.36.jar" DBCoursework

Or by running ./build.sh, shell file for windows that was produced for easier 
and simpler testing, which performs all tasks on its own.

Also, 
References:
- The CSV_Reader class is an adaptation from Lab 6, Week 17's "NativeCSVReader.java" from Lancaster University Leipzig, reimplemented for reading this Class's CSV files.
- Schmitz, G. (2024) "NativeCSVReader" (Version 1.0) [Source code]. Lancaster University Leipzig.

- All data retrieved for csv usage is from the Formula 1 website: https://www.formula1.com/
- Formula 1 website. (2024). Results, 2023. Formula 1® - The Official F1® Website. https://www.formula1.com/en/results.html/2024/races.html