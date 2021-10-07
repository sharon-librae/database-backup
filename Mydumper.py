import os
import time
import commands
import shutil
import threading
from os.path import join, getsize
import argparse
import oss2

# backup date
backup_date = time.strftime("%Y%m%d")


def main(database, tables_list, outputdir, statement_size, rows, chunk_filesize, compress, build_empty_files, regex, ignore_engines,
         no_schemas, no_locks, less_locking, long_query_guard, kill_long_queries, binlogs, daemon,
         snapshot_interval, logfile, tz_utc, skip_tz_utc, use_savepoints, success_on_1146, host,
         user, password, port, socket, threads, compress_protocol, version, verbose, accesskeyid, accesskeysecret, bucket):
    thread_pool = []

    # build backup folder
    dir = outputdir
    if (not os.path.exists(dir)):
        os.makedirs(dir)

    dir += "/%s%s" % (host, backup_date)
    # business folder: dir , directory: dir/host+data
    strcmd = buildcmd(database, tables_list, dir, statement_size, rows, chunk_filesize, compress, build_empty_files,
                      regex, ignore_engines, no_schemas, no_locks, less_locking, long_query_guard, kill_long_queries,
                      binlogs, daemon, snapshot_interval, logfile, tz_utc, skip_tz_utc, use_savepoints, success_on_1146,
                      host, user, password, port, socket, threads, compress_protocol, version, verbose)

    th = threading.Thread(target=mydumper,
                          args=(strcmd, dir, host, accesskeyid, accesskeysecret, bucket, outputdir))
    thread_pool.append(th)

    if (thread_pool):
        for t in thread_pool:
            t.daemon = True
            t.start()
        for t in thread_pool:
            t.join()


def buildcmd(database, tables_list, outputdir, statement_size, rows, chunk_filesize, compress, build_empty_files, regex,
            ignore_engines, no_schemas, no_locks, less_locking, long_query_guard, kill_long_queries, binlogs, daemon,
            snapshot_interval, logfile, tz_utc, skip_tz_utc, use_savepoints, success_on_1146, host,
            user, password, port, socket, threads, compress_protocol, version, verbose):

    cmd = "/usr/local/bin/mydumper "
    if database != "":
        cmd = cmd + "--database " + database + " "
    if tables_list != "":
        cmd = cmd + "--tables-list " + tables_list + " "
    if outputdir != "":
        cmd = cmd + "--outputdir " + outputdir + " "
    if statement_size != "":
        cmd = cmd + "--statement-size " + statement_size + " "
    if rows != "":
        cmd = cmd + "--rows " + rows + " "
    if chunk_filesize != "":
        cmd = cmd + "--chunk-filesize " + chunk_filesize + " "
    if compress != "":
        cmd = cmd + "--compress " + compress + " "
    if build_empty_files != "":
        cmd = cmd + "--build-empty-files " + build_empty_files + " "
    if regex != "":
        cmd = cmd + "--regex " + regex + " "
    if ignore_engines != "":
        cmd = cmd + "--ignore-engines " + ignore_engines + " "
    if no_schemas != "":
        cmd = cmd + "--no-schemas " + no_schemas + " "
    if no_locks != "":
        cmd = cmd + "--no-locks " + no_locks + " "
    if less_locking !="":
        cmd = cmd + "--less-locking " + less_locking + " "
    if long_query_guard != "":
        cmd = cmd + "--long-query-guard " + long_query_guard + " "
    if kill_long_queries != "":
        cmd = cmd + "--kill-long-queries " + kill_long_queries + " "
    if binlogs !="":
        cmd = cmd + "--binlogs " + binlogs + " "
    if daemon != "":
        cmd = cmd + "--daemon " + daemon + " "
    if snapshot_interval != "":
        cmd = cmd + "--snapshot-interval " + snapshot_interval + " "
    if logfile != "":
        cmd = cmd + "--logfile " + logfile + " "
    if tz_utc != "":
        cmd = cmd  + "--tz-utc " + tz_utc + " "
    if skip_tz_utc != "":
        cmd = cmd + "--skip-tz-utc " + skip_tz_utc + " "
    if use_savepoints != "":
        cmd = cmd + "--use-savepoints " + use_savepoints + " "
    if success_on_1146 != "":
        cmd = cmd + "--success-on-1146 " + success_on_1146 + " "
    if host != "":
        cmd = cmd + "--host " + host + " "
    if user !="":
        cmd = cmd + "--user " + user + " "
    if password != "":
        cmd = cmd + "--password " + password + " "
    if port != "":
        cmd = cmd + "--port " + port + " "
    if socket != "":
        cmd = cmd + "--socket " + socket + " "
    if threads != "":
        cmd = cmd + "--threads " + threads + " "
    if compress_protocol !="":
        cmd = cmd + "--compress-protocol " + compress_protocol + " "
    if version != "":
        cmd = cmd + "--version " + version + " "
    if verbose != "":
        cmd = cmd + "--verbose " + verbose

    return cmd


def mydumper(sCmd, backupDir, host, accesskeyid, accesskeysecret, bucket, outputdir):

    backup_host = host
    file = ""

    # delete left backup
    if (os.path.exists(backupDir)):
        shutil.rmtree(backupDir)

    # execute backup
    returncode, std_err = execute(sCmd)

    if (returncode == 0):
        # std_err should return ""
        if (std_err.strip() != ""):
            returncode = 123456
        else:
            # check backup file is invaild
            returncode, std_err, master_host, slave_statement = statement(backupDir, backup_host)

            if (returncode == 0):
                file = backupDir
                oss(accesskeyid, accesskeysecret, bucket, file, outputdir)
    else:
        errDir = backupDir + "_ERR"
        os.rename(backupDir, errDir)
        file = errDir
        print("Backup error")


def getDirsize(path):
    # get backup file size
    size = 0L
    for root, dirs, files in os.walk(path):
        size += sum([getsize(join(root, name)) for name in files])
    return (size)


def statement(path, backup_host):

    isMaster = 1
    path += "/metadata"
    sMetadata = ""
    master_host = ""
    er_code = 654321
    er_info = "%s not exists !!!" % (path)

    if (os.path.exists(path)):
        if (isMaster != 1):
            # backup host is slave
            num = 3
            sFinds = "SLAVE STATUS"
        else:
            num = 2
            sFinds = "MASTER STATUS"

        f = open(path, 'r')
        rows = f.readlines()
        i = 100
        lst = []
        for s in rows:
            if (s.find(sFinds) > 0):
                i = 1
                continue

            if (i <= num):
                lst.append(s.split(':')[1].strip())
                i += 1

        if (isMaster == 1):
            # backup host is master
            master_host = backup_host
            log_file, log_pos = lst
        else:
            # backup host is slave
            master_host, log_file, log_pos = lst

        er_code = 0
        er_info = ""
        sMetadata = "CHANGE MASTER TO MASTER_HOST='%s',MASTER_LOG_FILE='%s',MASTER_LOG_POS=%s,MASTER_USER='rep_user'," \
                    "MASTER_PASSWORD='meizu.com'" % (master_host, log_file, log_pos)

    return (er_code, er_info, master_host, sMetadata)


def execute(cmd):

    try:
        returncode, std_err = commands.getstatusoutput(cmd)
        return (returncode, std_err)
    except os.error, e:
        # error return 1001
        return (1001, e)


def oss(accesskeyid, accesskeysecret, bucket, file, host, outputdir):
    ossAuth = oss2.Auth(accesskeyid, accesskeysecret)
    ossBucket = oss2.Bucket(ossAuth, 'http://oss-cn-hangzhou.aliyuncs.com', bucket)
    list(file, ossBucket, host, outputdir)


def list(dir, ossbucket, host, outputdir):
    fs = os.listdir(dir)
    for f in fs:
        file = dir + '\\' + f

        if os.path.isdir(file):
            list(file, ossbucket, host)
        else:
            uploadFile(file, ossbucket, host, outputdir)


def uploadFile(file, ossbucket, host, outputdir):
    remoteName = host + file.replace(outputdir, '').replace('\\', '/')
    print ('uploading..', file, 'remoteName', remoteName)
    result = ossbucket.put_object_from_file(remoteName, file)
    print('http status: {0}'.format(result.status))



if __name__ == '__main__':

    # mydumper parameters
    parser = argparse.ArgumentParser(description='manual to this script')
    parser.add_argument("--database", type=str, default="")
    parser.add_argument("--tables-list", type=str, default="")
    parser.add_argument("--outputdir", type=str, default="/root/data/")
    parser.add_argument("--statement-size", type=str, default="")
    parser.add_argument("--rows", type=str, default="")
    parser.add_argument("--chunk-filesize", type=str, default="")
    parser.add_argument("--compress", type=str, default="")
    parser.add_argument("--build-empty-files", type=str, default="")
    parser.add_argument("--regex", type=str, default="")
    parser.add_argument("--ignore-engines", type=str, default="")
    parser.add_argument("--no-schemas", type=str, default="")
    parser.add_argument("--no-locks", type=str, default="")
    parser.add_argument("--less-locking", type=str, default="")
    parser.add_argument("--long-query-guard", type=str, default="")
    parser.add_argument("--kill-long-queries", type=str, default="")
    parser.add_argument("--binlogs", type=str, default="")
    parser.add_argument("--daemon", type=str, default="")
    parser.add_argument("--snapshot-interval", type=str, default="")
    parser.add_argument("--logfile", type=str, default="")
    parser.add_argument("--tz-utc", type=str, default="")
    parser.add_argument("--skip-tz-utc", type=str, default="")
    parser.add_argument("--use-savepoints", type=str, default="")
    parser.add_argument("--success-on-1146", type=str, default="")
    parser.add_argument("--host", type=str, default="")
    parser.add_argument("--user", type=str, default="")
    parser.add_argument("--password", type=str, default="")
    parser.add_argument("--port", type=str, default="")
    parser.add_argument("--socket", type=str, default="")
    parser.add_argument("--threads", type=str, default="")
    parser.add_argument("--compress-protocol", type=str, default="")
    parser.add_argument("--version", type=str, default="")
    parser.add_argument("--verbose", type=str, default="")
    parser.add_argument("--accessKeyId", type=str, default="")
    parser.add_argument("--accessKeySecret", type=str, default="")
    parser.add_argument("--bucket", type=str, default="")

    args = parser.parse_args()

    main(database=args.database, tables_list=args.tables_list, outputdir=args.outputdir,
         statement_size=args.statement_size, rows=args.rows, chunk_filesize=args.chunk_filesize, compress=args.compress,
         build_empty_files=args.build_empty_files, regex=args.regex, ignore_engines=args.ignore_engines,
         no_schemas=args.no_schemas, no_locks=args.no_locks, less_locking=args.less_locking,
         long_query_guard=args.long_query_guard, kill_long_queries=args.kill_long_queries, binlogs=args.binlogs,
         daemon=args.daemon, snapshot_interval=args.snapshot_interval, logfile=args.logfile, tz_utc=args.tz_utc,
         skip_tz_utc=args.skip_tz_utc, use_savepoints=args.use_savepoints, success_on_1146=args.success_on_1146,
         host=args.host, user=args.user, password=args.password, port=args.port, socket=args.socket,
         threads=args.threads, compress_protocol=args.compress_protocol, version=args.version, verbose=args.verbose,
         accesskeyid=args.accessKeyId, accesskeysecret=args.accessKeySecret, bucket=args.bucket)