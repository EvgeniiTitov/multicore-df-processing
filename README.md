Like it or not people keep using multiprocessing to distribute processing of
pandas DFs.

This is an attempt to unify/standardize the way to do it to avoid having
multiple instances of people doing it in different ways within one code base.

---
### TODO:

Minor:

- Worker.stop_worker() could block if queue is full

- Better test with more advanced pandas

- Comment your code

- Write tests

Major:

- Push provided DF to Apache Arrow's Object Store to avoid copying the DF to other processes