#!/bin/sh -e

if [ -z "$SNAPPY_JOB_SCRIPT" ]; then
  if type snappy-job.sh 2>/dev/null >/dev/null; then
    SNAPPY_JOB_SCRIPT="snappy-job.sh"
  elif [ -x "./snappy-job.sh" ]; then
    SNAPPY_JOB_SCRIPT="./snappy-job.sh"
  else
    echo "snappy-job.sh should be in PATH or current directory or set in SNAPPY_JOB_SCRIPT environment variable."
    exit 1
  fi
fi

if [ "$1" != "--lead" ]; then
  echo "Usage: <script> --lead <lead-host>:<port> --conf ..."
  exit 1
fi
LEAD="$2"
shift
shift

if ! curl -sS -X GET "http://$LEAD/jobs" >/dev/null; then
  exit 1
fi

RESULT=`$SNAPPY_JOB_SCRIPT submit --lead $LEAD --app-name filtered-backup --class io.snappydata.extensions.BackupDiskStore "$@" 2>/dev/null | sed 's/[^:]*{/{/'`
STATUS=`echo $RESULT | jq -r '.status'`

if [ "$STATUS" = "STARTED" -o "$STATUS" = "RUNNING" ]; then
  JOB_ID=`echo $RESULT | jq -r '.result.jobId'`
  echo Started backup job with ID $JOB_ID
  while [ "$STATUS" = "STARTED" -o "$STATUS" = "RUNNING" ]; do
    sleep 1
    STATUS=`$SNAPPY_JOB_SCRIPT status --lead $LEAD --job-id $JOB_ID 2>/dev/null | jq -r '.status'`
  done
  if [ "$STATUS" = "FINISHED" ]; then
    echo Job $JOB_ID completed successfully.
    echo
    $SNAPPY_JOB_SCRIPT status --lead $LEAD --job-id $JOB_ID 2>/dev/null | jq -r '.result'
    exit 0
  else
    echo Job $JOB_ID completed with status $STATUS
    echo
    $SNAPPY_JOB_SCRIPT status --lead $LEAD --job-id $JOB_ID 2>/dev/null | jq -r '.result'
    exit 1
  fi
else
  echo Job failed to start with status = $STATUS
  echo $RESULT
  exit 1
fi
