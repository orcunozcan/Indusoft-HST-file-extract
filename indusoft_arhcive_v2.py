import os
import subprocess
from tqdm import tqdm
import pandas as pd
import shutil

hst_folder_path = "D:\\Proje\\Çolakoğlu\\APM\\Automation Backups\\Su Tesisi Indusoft\\Hst" #HST Folder Location
hst2txt_exe_path = "HST2TXT.exe" #hst2txt.exe Location
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

df = pd.concat([df.iloc[:,0:3],df.iloc[:,3:].sort_index(axis="columns")],axis="columns")
filtered_data = [f for f in df.columns if search_filter.upper() in f.upper()]
filtered_data = ["Date","Time","Date-Time"] + filtered_data
df[filtered_data].to_csv("{}\\{}-{}_{}.csv".format(hst_folder_path,search_filter,firstDate,lastDate),index=False)

try:
    os.mkdir(os.path.join(hst_folder_path,"HDR_FILES"))
except:
    shutil.rmtree(os.path.join(hst_folder_path,"HDR_FILES"), ignore_errors=True)
    os.mkdir(os.path.join(hst_folder_path,"HDR_FILES"))
finally:
    for hdr in hdr_files:
        shutil.move(hdr,os.path.join(hst_folder_path,"HDR_FILES"))

try:
    os.mkdir(os.path.join(hst_folder_path,"TXT_FILES"))
except:
    shutil.rmtree(os.path.join(hst_folder_path,"TXT_FILES"), ignore_errors=True)
    os.mkdir(os.path.join(hst_folder_path,"TXT_FILES"))
finally:
    for txt in txt_files:
        shutil.move(txt,os.path.join(hst_folder_path,"TXT_FILES"))

try:
    os.mkdir(os.path.join(hst_folder_path,"CSV_FILES"))
except:
    shutil.rmtree(os.path.join(hst_folder_path,"CSV_FILES"), ignore_errors=True)
    os.mkdir(os.path.join(hst_folder_path,"CSV_FILES"))
finally:
    for csv in csv_file_name:
        shutil.move(csv,os.path.join(hst_folder_path,"CSV_FILES"))
