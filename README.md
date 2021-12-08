Like it or not people keep using multiprocessing to distribute processing of
pandas DFs.

This is an attempt to unify/standardize the way people do it to avoid having
multiple instances of people doing it in different ways within one code base.

TODO:

- Apache Arrow to avoid ugly IPC