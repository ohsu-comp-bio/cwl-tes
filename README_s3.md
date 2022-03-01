# tests for the s3 api



```
source ~/venv3/bin/activate
./cwl-tes --debug --remote-storage-url s3://celgene-rnd-riku-researchanalytics/funnel_1/ --tes http://localhost:8000 tests/hashsplitter-workflow.cwl.yml --input s3://celgene-rnd-riku-researchanalytics/funnel/tests/resources/test.txt
```




The above command works. Produces utput on s3 and a json output as such on stdout.

```
{
    "output": {
        "location": "s3://celgene-rnd-riku-researchanalytics/funnel_1/a7758d0f-a35f-4398-99c2-d6a955d71d76/unify/unify",
        "basename": "unify",
        "class": "File",
        "size": 472
    }
}

```



```
source ~/venv3/bin/activate
./cwl-tes --debug --remote-storage-url s3://celgene-rnd-riku-researchanalytics/funnel_1/ --tes http://localhost:8000 tests/hashsplitter-workflow.cwl.yml hashsplitter-input.json
```
works as well.


In stderr the line 

```
Submitting workflow: 171084b1-4faa-4dee-bc52-eb33c8084f2b
```

contains the id of the workflow

if a pipeline succeeds the script returns exit code 0

if a pipeline fails e.g.

```
source ~/venv3/bin/activate
./cwl-tes --debug --remote-storage-url s3://celgene-rnd-riku-researchanalytics/funnel_1/ --tes http://localhost:8000 tests/hashsplitter-workflow.cwl.yml hashsplitter-input_failed.json

```

exits with non-zero code



TODO


Check if the BucketFetcher is necessary. It does not seem to work i.e. when I add a sys.exit call the program does not stop.
