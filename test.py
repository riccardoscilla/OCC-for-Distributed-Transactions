import subprocess
import re
import pandas as pd

def get_percentage(val,tot):
    perc = round(val/tot*100,2)
    return str(val)+" ("+str(perc)+"%)"

def parse_file(df,fname):

    seed_pattern = re.compile(r"Seed: (?P<seed>\d+)")
    actor_pattern = re.compile(r"Actor Info: clients:(?P<clients>\d+) coordinators:(?P<coordinators>\d+) servers:(?P<servers>\d+)")
    init_txn_pattern = re.compile(r"CLIENT \d+ BEGIN")
    na_txn_pattern = re.compile(r"CLIENT \d+ TIMEOUT")
    commit_pattern = re.compile(r"CLIENT \d+ COMMIT (?P<commit>\w+) \(\d+\/\d+\)")
    abort_pattern = re.compile(r"CLIENT \d+ END ABORT")
    coordi_crash_pattern = re.compile(r"	COORDI \d+ Crashing")
    server_crash_pattern = re.compile(r"		SERVER \d+ Crashing")

    sum_pattern = re.compile(r"Final sum = (?P<sum>\d+) (?P<result>\w+)") 

    new_row = {}
    init_txn = 0
    na_txn = 0
    finished_txn = 0
    abort = 0
    commit_ok = 0
    commit_fail = 0
    coordi_crash = 0
    server_crash = 0

    with open(fname,'r') as f:
        for line in f:

            res = seed_pattern.match(line)
            if res:
                new_row["seed"] = res.groupdict()["seed"]
                continue

            res = actor_pattern.match(line)
            if res:
                
                new_row["clients"] = res.groupdict()["clients"]
                new_row["coordinators"] = res.groupdict()["coordinators"]
                new_row["servers"] = res.groupdict()["servers"]
                continue

            res = init_txn_pattern.match(line)
            if res:
                init_txn += 1
                continue

            res = na_txn_pattern.match(line)
            if res:
                na_txn += 1
                continue

            res = commit_pattern.match(line)
            if res:
                finished_txn += 1
                commit = res.groupdict()["commit"]
                if commit == "OK":
                    commit_ok += 1
                elif commit == "FAIL":
                    commit_fail += 1

            res = abort_pattern.match(line)
            if res:
                abort += 1

            res = coordi_crash_pattern.match(line)
            if res:
                coordi_crash += 1
            
            res = server_crash_pattern.match(line)
            if res:
                server_crash += 1

            res = sum_pattern.match(line)
            if res:
                new_row["sum"] = res.groupdict()["sum"]
                new_row["result"] = res.groupdict()["result"]
                continue
    
    new_row["init_txn"] = init_txn
    new_row["na_txn"] = na_txn
    new_row["finished_txn"] = finished_txn
    new_row["commit_ok"] = get_percentage(commit_ok,finished_txn)
    new_row["commit_fail"] = get_percentage(commit_fail - abort,finished_txn)
    new_row["abort"] = get_percentage(abort,finished_txn)
    new_row["coordi_crash"] = coordi_crash
    new_row["server_crash"] = server_crash

    df = df.append(new_row,ignore_index=True)
    return df

if __name__ == '__main__':

    n_sim = 1
    
    df = pd.DataFrame(columns= \
    ['seed', 'clients', 'coordinators', 'servers', 'init_txn', 'na_txn', 'finished_txn', \
    'commit_ok', 'commit_fail', 'abort', \
    "coordi_crash", "server_crash", \
    'sum', 'result'])

    for i in range(n_sim):
        print("starting sim",i+1,"...")

        cmd = "gradle run > log"+str(i+1)+".txt"
        subprocess.check_output(cmd, shell=True)

        print("finished test",i+1,"...")

        cmd = "java Check log"+str(i+1)+".txt > check"+str(i+1)+".txt"
        subprocess.check_output(cmd, shell=True)

        df = parse_file(df,"check"+str(i+1)+".txt")  

    print()
    print(df)