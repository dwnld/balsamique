Gem::Specification.new do |s|
  s.name = 'balsamique'
  s.version = '0.1.5'
  s.summary = 'Redis-backed Job Queue System'
  s.description = <<EOF
Balsamique (pronounced "Balsami-QUEUE") is a Redis-backed Ruby library
which implements a job queue system.  Balsamique jobs consist of
JSON-encoded args hashes, along with lists of tasks and their
successful outputs.  Jobs can be enqueued to run at some time in the
future, and workers can also delay the running of subsequent tasks.
Retries are automatically scheduled at the time a worker checks out a
job, and cancelled only when the worker reports success.  In contrast
to Resque, Balsamique uses Lua scripting in Redis extensively to make
job state transitions as atomic as possible.
EOF
  s.authors = ['DWNLD']
  s.email = 'keith@dwnld.me'
  s.homepage = 'https://github.com/dwnld/balsamique'
  s.licenses = ['MIT']
  s.files = ['lib/balsamique.rb', 'lib/balsamique/reporter.rb']
  s.add_runtime_dependency 'redis'
end
