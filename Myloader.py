import os
import time
import commands
import threading
import argparse

# backup date
backup_date = time.strftime("%Y%m%d")


def main(directory, queries_per_transaction, overwrite_tables, database, enable_binlog, host, user, password, port,
         socket, threads, compress_protocol, version, verbose):
    thread_pool = []

    # business folder: dir , directory: dir/host+data
    strcmd = buildcmd(directory, queries_per_transaction, overwrite_tables, database, enable_binlog, host, user,
                      password, port, socket, threads, compress_protocol, version, verbose)

    th = threading.Thread(target=myloader,
                          args=(strcmd))
    thread_pool.append(th)

    if (thread_pool):
        for t in thread_pool:
            t.daemon = True
            t.start()
        for t in thread_pool:
            t.join()


def buildcmd(directory, queries_per_transaction, overwrite_tables, database, enable_binlog, host, user,
             password, port, socket, threads, compress_protocol, version, verbose):

    cmd = "/usr/local/bin/myloader "
    if directory != "":
        cmd = cmd + "--directory " + directory + " "
    if queries_per_transaction != "":
        cmd = cmd + "--queries-per-transaction " + queries_per_transaction + " "
    if overwrite_tables != "":
        cmd = cmd + "--overwrite-tables " + overwrite_tables + " "
    if database != "":
        cmd = cmd + "--database " + database + " "
    if enable_binlog != "":
        cmd = cmd + "--enable-binlog " + enable_binlog + " "
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


def myloader(sCmd):

    # execute backup
    returncode, std_err = execute(sCmd)

    if (returncode == 0):
        # std_err should return ""
        if (std_err.strip() != ""):
            returncode = 123456
        else:
            print("load complete")
    else:
        print("load error")


def execute(cmd):

    try:
        returncode, std_err = commands.getstatusoutput(cmd)
        return (returncode, std_err)
    except os.error, e:
        # error return 1001
        return (1001, e)


if __name__ == '__main__':

    # mydumper parameters
    parser = argparse.ArgumentParser(description='manual to this script')
    parser.add_argument("--directory", type=str, default="")
    parser.add_argument("--queries-per-transaction", type=str, default="")
    parser.add_argument("--overwrite-tables", type=str, default="")
    parser.add_argument("--database", type=str, default="")
    parser.add_argument("--enable-binlog", type=str, default="")
    parser.add_argument("--host", type=str, default="")
    parser.add_argument("--user", type=str, default="")
    parser.add_argument("--password", type=str, default="")
    parser.add_argument("--port", type=str, default="")
    parser.add_argument("--socket", type=str, default="")
    parser.add_argument("--threads", type=str, default="")
    parser.add_argument("--compress-protocol", type=str, default="")
    parser.add_argument("--version", type=str, default="")
    parser.add_argument("--verbose", type=str, default="")

    args = parser.parse_args()

    main(directory=args.directory, queries_per_transaction=args.queries_per_transaction,
         overwrite_tables=args.overwrite_tables, database=args.database, enable_binlog=args.enable_binlog,
         host=args.host, user=args.user, password=args.password, port=args.port, socket=args.socket,
         threads=args.threads, compress_protocol=args.compress_protocol, version=args.version, verbose=args.verbose)