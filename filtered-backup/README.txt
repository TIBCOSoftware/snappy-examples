This directory contains code for a backup job that allows including/excluding diskstores.

It is to be built independently using "./gradlew build" with the jar being created inside "build/libs" directory. The project should also open in a recent Intellij IDEA.

This jar will need to be installed in the classpath of the nodes since backup also needs to run on locator and hence cannot use DEPLOY JAR or normal job execution way of jar distribution (which only goes to leads and servers). So you can either add the jar to classpath in conf/locators, conf/servers, conf/leads (-classpath=<jar path>) or copy to jars directory of the distribution -- former is better since you can keep the jar on a shared location rather than having to copy on all the nodes. Adding the jar to leads conf can be skipped in which case it needs to be passed to "--app-jar" option of "snappy-job.sh submit".

The following properties are provided:

backup.dir: location of the shared backup directory (required)
backup.baseline: location of the previous baseline directory for incremental backup
backup.includePattern: a case-insensitive regex pattern to specify only the disk stores to include (default is '.*')
backup.excludePattern: a case-insensitive regex pattern to specify the disk stores to exclude (default is empty)

An invocation that excludes the large table disk store named extData will look like:

snappy-job.sh submit --lead <lead-host>:8090 --app-name filtered-backup --class io.snappydata.extensions.BackupDiskStore --conf backup.dir=/backup/directory --conf backup.excludePattern='extdata|extdata-snappy-delta'

Add "--app-jar" argument if it is not in the lead classpath. This excludes both that disk store and its delta store (note the suffix "-snappy-delta"). The delta store is quite small so you can back it up, but the restore script will fail when it finds it to already exist in the target NFS location so it's best to skip it.

A sample script snappy-backup-job.sh is also included in the tarball at top-level which can be used as a reference for a cron job like execution that waits for the backup to complete. It requires the command-line JSON processor jq (on centos it can be installed with "yum install jq" assuming epel is enabled i.e. epel-release is installed, while on ubuntu/debian it is in standard repos). This script will submit the job, then wait for it to finish polling the status every second and then output the properly formatted result from the JSON status message. It will also show a failure message in case the job fails for some reason or fails to submit, and exits with proper error code for failure cases so you can use in cron on other places that rely on exit code. Invocation is like above but includes the app-name and class by default (this requires the --lead argument to be at the start):

snappy-backup-job.sh --lead <lead-host>:8090 --conf backup.dir=/backup/directory --conf backup.excludePattern='extdata|extdata-snappy-delta'

It expects to find snappy-job.sh in PATH or current directory or set into environment variable SNAPPY_JOB_SCRIPT i.e. export SNAPPY_JOB_SCRIPT=/path/.../snappy-job.sh

In case you want to customize the job code frequently, the build will have to be changed to create a separate jar for BackupDiskStore class and separate one for the other classes. The latter one will need to be on the classpath in conf files, while the former can be passed as "--app-jar" when submitting the job. This is so that you do not need to restart the cluster after every update of the job code.
