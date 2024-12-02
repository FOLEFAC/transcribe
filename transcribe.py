import os
from utils import fill_folders, initialization, clear_ipynb, get_states, saver
import sys
import pandas as pd
section = sys.argv[1]
chunk_size = int(sys.argv[2])

print("the section is: ", section)
main_path = "/content/drive/MyDrive/Cleeroute/Subtitles"
if os.path.exists(main_path+str(section)):
  print("Folders and subfolders have already been created")
else:
  fill_folders()

initialization(section)
print("Initialization Done !")

clear_ipynb(section)

input_files = os.listdir(main_path+str(section)+"/inputs")
input_files.sort()
done_files_list, done_files_dict = get_states(section)

for l, f in enumerate(input_files):
  clear_ipynb(section)
  update = False
  chunk = []
  print(main_path+str(section)+"/inputs/"+f)
  df = pd.read_csv(main_path+str(section)+"/inputs/"+f)
  try:
    status = done_files_dict[f][-4:]
    current_position = int(done_files_dict[f][:-4])
    update = True
  except:
    status = "none"
    current_position = 0

  if status == "none":
    print(current_position, status )

    for i,v in df.iterrows():
      if i>=current_position:
        chunk.append(v["video_id"])
      if i==len(df)-1:
        status = "done"
      else:
        status = "none"
      if (i%chunk_size == 0 and i>current_position) or status=="done":
        saver(chunk,f, i,status, update, section)
        update = True
        chunk = []