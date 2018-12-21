Word-Count (a.k.a. the "Hello, World!" for data analytics) counts the occurrences of each distinct word from a text file.

The "killer feature" in the PiCo API is that **there is no data**:
a PiCo application is described only in terms of *processing* (i.e., pipeline stages) -
rather than processing *and* data.

In `pico_wc.cpp`, the `wc` pipeline:

1. reads an input file line by line, by a `ReadFromFile` stage
2. tokenizes each line into words, by a `FlatMap` stage - a `FlatMap` maps an item (line) to multiple items (words)
3. maps each word `w` to a key-value pair <`w`,`1`>, by a `Map` stage
4. groups the pairs by word and sums up them, by a `ReduceByKey` stage
5. finally, the word-occurrences pairs <`w`,`n`> are written to an output file, by a `WriteToFile` stage

## Run the application
See the home [README](../../README.md) for build instructions.

Generate 1k random lines:

```bash
cd /path/to/build/examples/word-count
cd testdata
./generate_text dictionary.txt 1000 >> lines.txt
```

Count those words:
```bash
cd ..
./pico_wc testdata/lines.txt count.txt
```

:bulb: Parallelism degree can be set:
- externally, by the application-wise `PARDEG` environment variable
- within the code, for each operator, by passing an (optional) argument to operators' constructors;
per-operator parallelism overrides `PARDEG`

## See the application graph
Call the `to_dotfile()` function on a PiCo pipeline to produce a `dot` representation of its semantics.

To visualize the graph for the `wc` pipeline:

```bash
dot -Tpng word-count.dot -o word-count.png
```
