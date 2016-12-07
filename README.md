PiCo
===============

PiCo stands for **Pi**peline **Co**mposition.

The main entity in PiCo is the *Pipeline*, basically a DAG-composition of processing elements. 

This model is intended to give the user an unique interface for both stream and batch processing, hiding completely data management and focusing only on operations, which are represented by Pipeline stages. 

The DSL we propose is entirely implemented in C++11, exploiting the FastFlow library as runtime.

## How to compile
To build any PiCo application, the FastFlow library should be downloaded.

It can be downloaded from the [SourceForge repository](https://sourceforge.net/projects/mc-fastflow/), or just type

`$ svn checkout svn://svn.code.sf.net/p/mc-fastflow/code/ mc-fastflow-code`

and then add a symbolic link to the FastFlow library into the PiCo directory:

`$ ln -s /path/to/FastFlow/ff /PiCo/root/dir`

In `test` directory two example are present: a word count (`pico_wc.cpp`) and a merging pipelines (`pico_merge.cpp`).
To compile those tests:

`$ cd test`

`$ make`

This command will also launch the two examples: two OK messages confirm the correctness of the results.

To run each single example, just type:

`$ ./pico_merge <input file> <output file>`

or

`$ ./pico_wc <input file> <output file>`

Test input files can be found in `test/testdata/`.

### Graph Visualization
Each example produces a `.dot` file for a graphical representation of the application pipeline coded in Graphviz.
To visualize these graphs, use the command:

`dot -Tpng filename.dot -o outfile.png`
