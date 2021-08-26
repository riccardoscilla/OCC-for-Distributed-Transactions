import subprocess
import re
import pandas as pd

def parse_file(df, fname):

    seed_pattern = re.compile(r"Seed: (?P<seed>\d+)")
    actor_pattern = re.compile(r"Actor Info: clients:(?P<clients>\d+) coordinators:(?P<coordinators>\d+) servers:(?P<servers>\d+)")
    init_txn_pattern = re.compile(r"Client Order \[(?P<txn>.+)\]")
    tot_txn_pattern = re.compile(r"TxnId@\S+ \d+ \w+")
    commit_pattern = re.compile(r"CLIENT \d+ COMMIT (?P<commit>\w+) \(\d+\/\d+\)")
    sum_pattern = re.compile(r"Final sum = (?P<sum>\d+) (?P<result>\w+)") 

    new_row = {}
    tot_txn = 0
    commit_txn = 0
    fail_txn = 0

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
                txn = res.groupdict()["txn"].split(", ")
                new_row["init_txn"] = len(txn)
                continue

            # res = tot_txn_pattern.match(line)
            # if res:
            #     tot_txn += 1
            #     continue

            res = commit_pattern.match(line)
            if res:
                tot_txn += 1
                commit = res.groupdict()["commit"]
                if commit == "OK":
                    commit_txn += 1
                elif commit == "FAIL":
                    fail_txn += 1

            res = sum_pattern.match(line)
            if res:
                new_row["sum"] = res.groupdict()["sum"]
                new_row["result"] = res.groupdict()["result"]
                continue
    
    new_row["tot_txn"] = tot_txn
    new_row["commit_txn"] = commit_txn
    new_row["fail_txn"] = fail_txn

    df = df.append(new_row,ignore_index=True)
    return df
    
if __name__ == '__main__':

    n_sim = 1
    
    df = pd.DataFrame(columns= \
    ['seed', 'clients', 'coordinators', 'servers', 'init_txn', 'tot_txn', 'commit_txn', 'fail_txn', 'sum', 'result'])

    for i in range(n_sim):
        print("starting sim",i+1,"...")

        cmd = "gradle run > log.txt"
        subprocess.check_output(cmd, shell=True)

        cmd = "java Check log.txt > check.txt"
        subprocess.check_output(cmd, shell=True)

        df = parse_file(df,"check.txt")

    print()
    print(df)


