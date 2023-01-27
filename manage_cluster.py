import os
import subprocess

def launch_hadoop():
    subprocess.call(["start-all.sh"])

def create_cluster(cluster_name:str='datalake'):
    subprocess.call(["hadoop", "fs" "-mkdir", f"/{cluster_name}"])
    for filename in os.listdir("./data"):
        subprocess.call(["hadoop", "fs", "-copyFromLocal", f"./data/{filename}", f"/{cluster_name}"])

launch_hadoop()
create_cluster()
