# Problems with Spark (just to run it)

1. **Java version**: Java 21 is still expermintal and Spark does not support it. You need to install Java 8 or 11 or 17.
2. Environment variables: You need to set the environment variables for Java, Hadoop (winutils.exe) and Spark.
3. Logging: Spark uses log4j for logging. You need to set the log4j properties file.
   1. There a problem here of exception that you have to write to be able to work
4. 