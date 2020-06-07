## Install requirements:

```
pip install -r requirements.txt
```

## Usage:

**Requires python 3.7+**
```
aionomadlog.py TASKNAME [OPTIONS]
```

where TASKNAME is a glob-like pattern which is used to search for matching task names, e.g.:

```
python3 aionomadlog.py MyTask_* -h 127.0.0.1 -p 4646
```

## Help:

```
python3 aionomadlog.py --help
```
