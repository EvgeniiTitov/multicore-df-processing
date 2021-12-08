Like it or not people keep using multiprocessing to distribute processing of
pandas DFs.

This is an attempt to unify/standardize the way to do it to avoid having
multiple instances of people doing it in different ways within one code base.

---
###TODO:

Minor:

- User provided batch size is not met (should a user provide the number of splits?)

- Dont start more workers than required (splits), move their initialization to .process_df()

- Worker.stop_worker() could block if queue is full

- Better test with more advanced pandas

- Add progress bar to show how many partitions processed

- Measure df size in the main process VS sliced df that a worker receives - ensure only the subset gets copied

- Comment your code

- Write tests

Major:

- Push provided DF to Apache Arrow's Object Store to avoid copying the DF to other processes