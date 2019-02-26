A PiCo implementation of the well-known [PageRank](https://en.wikipedia.org/wiki/PageRank) algorithm.

The code in `pagerank.cpp` shows the usage of **iterative pipelines**.

## Run the application
See the home [README](../../README.md) for build instructions.

This simplified implementation takes in input two text files:

-   a list of pages, one per line
-   a list of edges, one per line, where each edge is a pair of space-separated pages

and produces a list of page-rank pairs.

The `prg-convert` utility converts edge-only graphs to their pages+edges representation.
Grab a graph from the [SNAP repository](https://snap.stanford.edu/data/index.html) and convert it:

```bash
cd /path/to/build/examples/page-rank/testdata
wget https://snap.stanford.edu/data/cit-HepPh.txt.gz
gunzip cit-HepPh.txt.gz
./prg-convert cit-HepPh.txt foo
```

Compute the ranks:
```bash
cd ..
./pagerank testdata/foo-nodes testdata/foo-edges bar
```

## See the application graph
To visualize the graph for the `pageRank` pipeline:

```bash
dot -Tpng page-rank.dot -o page-rank.png
```
