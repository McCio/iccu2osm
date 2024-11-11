
env:
	python3 -m venv venv
	venv/bin/python3 -m pip install --upgrade pip
	venv/bin/python3 -m pip install --requirement requirements.txt

update:
	curl --etag-save data/iccu.etag --etag-compare data/iccu.etag --silent -o data/iccu.zip https://opendata.anagrafe.iccu.sbn.it/opendata.zip
	bsdtar -x -f data/iccu.zip -C data/

process:
	venv/bin/python3 coalesce.py
