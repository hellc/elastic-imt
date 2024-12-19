# elastic-imt
Elastic Index Migration Tools


<img src="/imgs/indexer.jpg?raw=true" alt="Indexer" title="Indexer" width="42%" height="42%"> <img src="/imgs/dumper.jpg?raw=true" alt="JSONL Dumper" title="JSONL Dumper" width="40%" height="40%">


### Install dependencies
```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Web UI based on streamlit to migrate documents from one Elasticsearch index to another or dump documents from an index to JSONL files.
#### Init command:

```
streamlit run src/app.py
```
Then open Local URL: http://localhost:8501 in browser
