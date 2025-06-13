# CRDT_fs
223B final project, filesystem built on CRDT




macOS

Install fusepy (pyfuse3 is only compatible with Linux):
brew install --cast macfuse
pip install fusepy

Install requirements:
pip3 install -r requirements.txt

Make a config.json file and add:
{"replica":1142804968,"peers":[],"basepath":"/home/xinle/data","mountpoint":"/home/xinle/mnt","host":"0.0.0.0","port":8000}

Run main.py on the config:
x64: python3 src/main.py config.json 
ARM (M1-M4): arch -arm64 python3 src/main.py config.json



