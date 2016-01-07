#!/usr/bin/env ruby

require 'logger'
require 'cassandra'

$stdout.sync = true

logger = Logger.new(STDOUT)
logger.level = Logger::INFO
logger.formatter = proc do |severity, datetime, progname, msg|
  "[#{datetime}] #{severity} creator-cassandra: #{msg}\n"
end

if ENV['CREATOR_CASSANDRA_DATABASES']
  dbs = ENV.fetch('CREATOR_CASSANDRA_DATABASES').split('|')
else
  logger.info { %|=> CREATOR_CASSANDRA_DATABASES variable does not exist! Bailing.| }
  exit 1
end

tries ||= ENV.fetch('CREATOR_CASSANDRA_TRIES', 1).to_i
begin
  logger.info { %|=> Connecting to #{ENV.fetch('CREATOR_CASSANDRA_HOST', 'localhost')}...| }
  c = Cassandra.cluster(
    username: ENV.fetch('CREATOR_CASSANDRA_ROOT_USER', 'cassandra'),
    password: ENV.fetch('CREATOR_CASSANDRA_ROOT_PASS', 'cassandra'),
    hosts: [ENV.fetch('CREATOR_CASSANDRA_HOST', 'localhost')],
    logger: logger
  )

  session  = c.connect

  dbs.each do |db|
    create = [%|CREATE KEYSPACE IF NOT EXISTS "#{db}" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };|]
    create << %|CREATE USER IF NOT EXISTS #{ENV.fetch('CREATOR_CASSANDRA_USER', 'dev')} WITH PASSWORD '#{ENV.fetch('CREATOR_CASSANDRA_PASS', 'dev')}' NOSUPERUSER;|
    create << %|GRANT ALL ON KEYSPACE #{db} TO #{ENV.fetch('CREATOR_CASSANDRA_USER', 'dev')};|
    create << %|GRANT SELECT ON TABLE system.size_estimates TO #{ENV.fetch('CREATOR_CASSANDRA_USER', 'dev')};|
    create << %|GRANT CREATE ON ALL KEYSPACES TO #{ENV.fetch('CREATOR_CASSANDRA_USER', 'dev')};|

    logger.info('creator-cassandra') { %| => Setting up #{db}...| }
    create.each { |query| session.execute(query) }
    logger.info { %|=> ...done.| }
  end
  session.close
  c.close
  logger.close unless ENV['CREATOR_MARATHON']

  if ENV['CREATOR_MARATHON']
    # Do not exit on success under Marathon
    logger.info { %|=> CREATOR_MARATHON specified, sleeping forever.| }
    logger.close
    sleep
  end
rescue Cassandra::Errors::AuthenticationError => ex
  logger.info { %|=> #{ex.message}| }
rescue Cassandra::Errors::NoHostsAvailable => ex
  logger.warn { %|=> #{ex.message}| }
  logger.warn { %|=> Sleeping for #{ENV.fetch('CREATOR_CASSANDRA_SLEEP', 10)} seconds, attempts left #{tries}/#{ENV.fetch('CREATOR_CASSANDRA_TRIES', 1)}| }
  sleep ENV.fetch('CREATOR_CASSANDRA_SLEEP', 10).to_i
  retry unless (tries -= 1).zero?
ensure
  logger.close
end
