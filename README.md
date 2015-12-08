balsamique
==========

Balsamique (pronounced "Balsami-QUEUE") is a Redis-backed Ruby library
which implements a job queue system.  Balsamique jobs consist of
JSON-encoded args hashes, along with lists of tasks and their
successful outputs.  Jobs can be enqueued to run at some time in the
future, and workers can also delay the running of subsequent tasks.
Retries are automatically scheduled at the time a worker checks out a
job, and cancelled only when the worker reports success.  In contrast
to Resque, Balsamique uses Lua scripting in Redis extensively to make
job state transitions as atomic as possible.
