Gem::Specification.new do |s|
  s.name = 'balsamique'
  s.version = '0.1.0'
  s.date = '2014-10-28'
  s.summary = 'Redis-backed Job Queue System'
  s.description = <<EOF
Balsamique (pronounced "Balsami-QUEUE") is a Redis-backed Ruby library
which implements a job queue system.  Balsamique jobs consist of lists
of tasks.  Jobs can be enqueued to run at some time in the future, and
workers can also delay the running of subsequent tasks.  In contrast
to Resque, Balsamique uses Lua scripting in Redis to make job state
transitions as atomic as possible.  Balsamique comes with a Sinatra
application, which not only provides dashboard functionality, but
allows remote workers to connect and authenticate themselves via SSL.
The Sinatra application can also be configured to hand out pre-signed
GET and POST request for an AWS S3 bucket, allowing workers to store
and retrieve files keyed by job id.
EOF
  s.authors = ['DWNLD']
  s.email = 'keith@dwnld.me'
  s.homepage = 'https://github.com/dwnld/balsamique'
  s.licenses = ['MIT']
  s.files = ['lib/balsamique.rb']
  s.add_runtime_dependency 'redis'
end
