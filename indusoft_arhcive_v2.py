import os
import subprocess
from tqdm import tqdm
import pandas as pd

hst_folder_path = "C:\\Users\\Orcun\\Desktop\\Proje\\CM-APM\\Automation Backup\\Colakoglu_Compresor_Tracing\\Hst\\" #HST Folder Location
hst2txt_exe_path = hst_folder_path + "HST2TXT.exe" #hst2txt.exe Location
cyclic = 3600 #SECOND
search_filter = "INGERSOLLAND" #SEARCH
hst_seperator = "\t" #Hst output csv seperator
file_exist=os.path.exists(hst2txt_exe_path)

hst_files = [os.path.join(hst_folder_path,f) for f in os.listdir(hst_folder_path) if f.endswith(".hst")]

bar = tqdm(hst_files,unit="file")
for file in bar:
    bar.set_description("File is processing : {}".format(file))
    subprocess.call([hst2txt_exe_path,file,"/l:{}".format(cyclic),hst_seperator,"/e"],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.STDOUT)

hdr_files = [os.path.join(hst_folder_path,f) for f in os.listdir(hst_folder_path) if f.endswith(".hdr")]
txt_files = [f.replace(".hdr",".txt") for f in hdr_files]

i=0
bar = tqdm(hdr_files,unit="file")
csv_file_name = []
for csv_file in bar:
    bar.set_description("File is processing out to csv: {}".format(csv_file))
    with open(csv_file) as f:
        lines = f.readlines()
        data = [line.rstrip() for line in lines]
        del data[0]
        data = ["Date","Time"] + data
        df = pd.read_csv(txt_files[i],sep="\s+",header=None,names=data)
        date_time = pd.to_datetime(df['Date'] + df['Time'], format='%d/%m/%Y%H:%M:%S')
        df.insert(2,"Date-Time",date_time)
        # df["Date-Time"] = pd.to_datetime(df["Date-Time"].dt.strftime('%Y-%m'))
        df.to_csv("{}".format(csv_file.replace(".hdr",".csv")),index=False)
        csv_file_name.append("{}".format(csv_file.replace(".hdr",".csv")))
    i+=1

# csv_files = [f for f in os.listdir(".") if f.endswith(".csv")]
csv_file_name.sort()
df = pd.concat(map(pd.read_csv, csv_file_name), ignore_index=True)
firstDate = os.path.basename(csv_file_name[0].replace(".csv",""))
lastDate = os.path.basename(csv_file_name[-1].replace(".csv",""))
df.to_csv("{}\\{}_{}.csv".format(hst_folder_path,firstDate,lastDate),index=False)

filtered_data = [f for f in df.columns if search_filter.upper() in f.upper()]
filtered_data = ["Date","Time","Date-Time"] + filtered_data

df[filtered_data].to_csv("{}\\{}-{}_{}.csv".format(hst_folder_path,search_filter,firstDate,lastDate),index=False)
