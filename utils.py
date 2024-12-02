import time
import os
import multiprocessing
import csv
import yt_dlp
import requests
import pandas as pd
import concurrent.futures
import urllib.request
from tempfile import NamedTemporaryFile
import shutil
import csv
import xml.etree.ElementTree as ET
import urllib.request
from xml.dom.minidom import parseString


main_path = "/content/drive/MyDrive/Cleeroute/Subtitles"
input_path = main_path+"/inputs/"

def get_text_from_xml(xml_data):
    root = ET.fromstring(xml_data)

    text_segments = []

    for text_elem in root.findall(".//text"):
        text = text_elem.text
        if text is not None:
            text = text.strip()
            if text != "[Music]":
                text_segments.append(text)

    all_text = " ".join(text_segments)
    return all_text

def gen_word_groups(sentence, group_size):
    word_groups = []
    words = sentence.split()

    for i in range(0, len(words), group_size):
        group = " ".join(words[i : i + group_size])
        word_groups.append(group)
    return word_groups

def get_output_path(p):
    return main_path+str(p)+"/inputs/"

def break_list_into_parts(original_list):
    length = len(original_list)
    part_length = length // 3

    part1 = original_list[:part_length]
    part2 = original_list[part_length:2 * part_length]
    part3 = original_list[2 * part_length:]
    return part1, part2, part3

def create_folders(folder1, folder2, folder3):

    # Create the three main folders
    os.makedirs(folder1, exist_ok=True)
    os.makedirs(folder2, exist_ok=True)
    os.makedirs(folder3, exist_ok=True)

    # Iterate through the three main folders
    for main_folder in [folder1, folder2, folder3]:
        # Create the "inputs" and "outputs" folders inside each main folder
        os.makedirs(os.path.join(main_folder, "inputs"), exist_ok=True)
        os.makedirs(os.path.join(main_folder, "outputs"), exist_ok=True)

    print("3 folders with 'inputs' and 'outputs' subfolders created successfully.")
    return None

def fill_folders():
    create_folders(main_path+"1", main_path+"2", main_path+"3")
    input_list = os.listdir(input_path)
    input_list.sort()

    part1, part2, part3 = break_list_into_parts(input_list)
    print("Copying files...")
    for i, p in enumerate([part1,part2,part3]):
      for f in p:
        shutil.copyfile(input_path+f, get_output_path(i+1)+f)
    print("Copy Done !")
    return None

def initialize_file(section):

    done_files = main_path+str(section)+"/done_files.csv"
    if os.path.exists(done_files):
      pass
    else:
      with open(done_files, 'w', newline='',  encoding='utf-8') as csvfile:
          fieldnames = ['csv_file','position','status']
          write = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL, escapechar='\\')
          write.writerow(fieldnames)
    return None

def initialize_folders(section):
    output_files = os.listdir(main_path+str(section)+"/outputs/")
    for f in os.listdir(main_path+str(section)+"/inputs/"):
      if f not in output_files:
        with open(main_path+str(section)+"/outputs/"+f, 'w', newline='',  encoding='utf-8') as csvfile:
            fieldnames = ['video_id', 'subtitle']
            write = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL, escapechar='\\')
            write.writerow(fieldnames)
    return None

def initialization(section):
    initialize_file(section)
    initialize_folders(section)
    return None

def clear_ipynb(section):
    try:
      os.rmdir(main_path+str(section)+"/inputs/.ipynb_checkpoints")
      os.rmdir(main_path+str(section)+"/outputs/.ipynb_checkpoints")
    except:
      pass
    return None
def get_youtube_transcript(video_url):
    ydl_opts = {
        'verbose': False,
        'quiet': True,
        'sub_format': 'vtt',
        'writesubtitles': True,
    }
    try:
      with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(video_url, download=False)
        transcript = info.get('automatic_captions', {}).get('en', None)
        if transcript:
          for t in transcript:
            if t['ext'] == 'srv1':
              srv_transcript = t['url']
              #print("transcript--->:", srv_transcript)
              return srv_transcript
          return "NotFound"
        else:
          #print("Transcript not available for this video.")
          return "NotFound"
    except Exception as e:
      print("Error--->", e)
      return "NotFound"
def scrape_worker(url):
    return [url, get_youtube_transcript(url)]

def get_states(section):
    done_files_df = pd.read_csv(main_path+str(section)+"/done_files.csv",
                      sep=',',
                      names = ["csv_file", "position", "status"], encoding="utf-8")
    done_files_dict = {}
    done_files_list = []

    for i,f in done_files_df.iterrows():
      done_files_dict[f['csv_file']] = str(f['position'])+f['status']
      done_files_list.append(f['csv_file'])
    return done_files_list, done_files_dict

def saver(urls, filepath, pos, status, update, section):
    clear_ipynb(section)
    data = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        future_to_url = {executor.submit(scrape_worker, url): url for url in urls}
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            data.append(future.result())

    expanded_data = []

    for item in data:
      if item[1] == "NotFound":
        pass
      else:

        try:
          file = urllib.request.urlopen(item[1])
          data = file.read()
        except:
          print("$$$$$$$$$$$$$$$$$$$$$$$$$ - Error 429 detected !!! - $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
          response = requests.get(item[1])
          if response.status_code == 200:
              data = response.content
              print("NOw worksssssssssssssssssssssssssssss !!!!!!!!!!!")
          else:
              print("Still not working out !!!!!!!!!!!!!!!!", response.status_code)
              
        text = get_text_from_xml(data)
        text_groups = gen_word_groups(text, 500)
        for t in text_groups:
          expanded_data.append([item[0], t])

    with open(main_path+str(section)+"/outputs/"+filepath, "a", newline="", encoding='utf-8') as file:
        csv_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL, escapechar='\\')
        csv_writer.writerows(expanded_data)
    if update == False:
      print("Updating... false")
      df = pd.read_csv(main_path+str(section)+"/done_files.csv",
                      sep=',',
                      names = ["csv_file", "position", "status"], encoding="utf-8")
      df.loc[-1] = [filepath, pos, status]
      df_final = df[['csv_file', 'position', 'status']]
      df_final.to_csv(main_path+str(section)+"/done_files.csv",
                      sep=',', encoding='utf-8')
    else:
      print("Updating... true")

      df = pd.read_csv(main_path+str(section)+"/done_files.csv")
      df.loc[df["csv_file"] == filepath, 'position'] = pos
      df.loc[df["csv_file"] == filepath, 'status'] = status

      df_final = df[['csv_file', 'position', 'status']]
      df_final.to_csv(main_path+str(section)+"/done_files.csv",
                      sep=',', encoding='utf-8')
    return None