# MS3InterviewChallenge

# Purpose
This project's goal is to efficiently parse the data given by a CSV file and write the valid records to an SQLite database.  In the process, the program should also filter out bad/invalid records and write them to a seperate CSV and then record the results into a log file.

# Approach and Design
Although I used maven and a couple open-source libraries to facilitate development, the majority of my focus was on speed.  I started out by simply writing each record to the DB one at a time, which took over 13 minutes to run.  I explored the idea of using multithreading to speed this up, but abandoned that approach when I realized it would be impossible to use multiple threads without knowing the exact number of records in the CSV which is not possible with FileReaders.  I then tried using batch inserts.  My initial design had two methods, one that inserted records one at a time, and one that attempted to insert a batch (lets say 30) at a time, and then when a batch failed, it would put the batch throught the one-at-a-time method.  This shortened the runtime to about 4 minutes.  The next issue was that it was taking a lot of time to verify that the records were non-empty, in fact it was using a tripley-nested loop to do so.  So my next idea was to pre-validate all of the records that were non-empty and write them to a seperate CSV (verified-records.csv).  This not only shortened processing time for verifying records, but also shortened it by greatly reducing the number of bad records in each batch. This shortened processing to about 2 minutes. I decided that once the bad query in a batch was identified by the slower method, it should try to reinsert the rest of the batch into the batch method, so I overloaded the method to implement a recursive solution.  Finally, with the right batch size, pre-writing valid records to a file, and recursivly batch inserting, I achieved a best time of 26 seconds with a batch size of 35. 

# Acknowledgements
* I am unsure how to determine the optimal batch size without first knowing at least the prominence of invalid records (or rather the ratio of good:bad)
* While maven is used to import the open-source libraries needed for file parsing and connecting to the DB, I had trouble getting it to run, so I ran my code as a java project in eclipse after maven built it.
* I believe my algorithm could be improved by instead of using the slow method to find a bad file and then try reinsterting, I used a sort of binary search technique where if a batch failed I would try half of that batch, and then another half, and so on recursivly, although with SOME modification based on known prevelance of invalid records
* I tried to keep my code as clean and readable as possible, although I did so much last-minute tweeking, that it could certainly be a lot better after a few hours of refactoring. 

# Steps for Getting the App to Run
This is by far the least elegant part of my project as I tried to use Maven for the first time and had many problems
* Clone URI and import into eclipse using git smart import. 
* maven clean install
* need to Delete all records from DB before running as the DB in the repo is full
* run as a java project, not a maven build (still need to maven clean install to get libraries) 
* need to send DELETE FROM purchases; to purchases.db in order to re-run or all of the records will be rejected. I used a DBMS to do this but you could also send it in the code. 
