import json
import os
import shutil
import fileinput

curr_folder_path = os.path.dirname(os.path.abspath(__file__))
dags_folder_path = os.path.dirname(curr_folder_path)
config_folder_path = os.path.join(curr_folder_path, "dag_config")
dag_template_file_path = os.path.join(curr_folder_path, "dag_template.py")

for config_filename in os.listdir(config_folder_path):
    config_file = open(os.path.join(config_folder_path, config_filename))
    config = json.load(config_file)

    new_filename = os.path.join(dags_folder_path, config["dag_id"] + ".py")
    shutil.copyfile(dag_template_file_path, new_filename)

    for line in fileinput.input(new_filename, inplace=True):
        line = line.replace("dag_id_to_replace", config["dag_id"])
        line = line.replace("src_file_name_to_replace", config["src_file_name"])
        line = line.replace("tgt_table_name_to_replace", config["tgt_table_name"])
        print(line, end="")

    print(f"Generated the file: '{new_filename}'")
