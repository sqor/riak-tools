# riak-tools
Backup and restore a riak node into an AWS s3 bucket.

##Build
`make`

or to create executable:

`make escript` which creates an executable called "riak-tools"

##Test
`make eunit`

##Run

`./riak-tools backup`


Riak_tools is a self-contained erlang escript, which backs up the local leveldb data from /var/lib/riak.  To
best understand the code's behavior, please read the source code!

The backup command can be called via crontab or jenkins.  For production, backups are required hourly.  Because
backups could fill the local disk, accidentally start a riak node, and other undesirable behavior, the code
makes a best effort at paranoid safe behavior.  It is safe to repeatedly call backup.  Existing local copies will
be overwritten with the new backup.  Backup checks to make sure Riak is running prior to backup and is restarted
after the backup completes.  If riak is not running, backup will abort.

The s3 bucket must be versioned.  Because s3 already versions the backup, the backup's name need only be uniquely
identified by its hostname and the month number.  This ensures we have unique backups going back 12 months for
each Riak node.

Care is taken to ensure 

Details

- Backups are stored in three locations: local disk, s3 and Glacier.  (Future: offsite backup copies).
- A backup is a tar gzipped archive of /var/lib/riak and /etc/riak
- Each backup is named <hostname>-riakdb.tgz
- Backups are not uniquely named (except for hostname).  This allows s3 versioning to work.
- A local hardlink is made to <hostname>-riakdb.tgz.month.<num> and <hostname>-riakdb.tgz.wday.<num>
- The *.month.* and *.wday.* copies ensure that we have backup copies going back 12 months and 7 days.
- The s3 bucket is versioned.
- The s3 bucket is replicated to Glacier.  Note that Glacier recovery may take several hours.  Glacier
  is useful for emergencies in case the s3 bucket itself is deleted.

##Jenkins Jobs
http://build.sqor.com/job/DEV_riak_archive/
