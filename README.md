# elastic-imt
Elastic Indexes Migration Tools

### Install dependencies
```
python3 -m venv .venv
source .venv/bin/activate
pip install -r req.txt
```

### Web UI based on streamlit to migrate documents from one Elasticsearch index to another or dump documents from an index to JSONL files.
#### Init command:

```
streamlit run src/app.py
```