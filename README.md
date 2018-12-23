PiCo: High-Performance Data-Analytics Pipelines in C++
===============
[![Travis Build Status](https://travis-ci.org/alpha-unito/PiCo.svg?branch=master)](https://travis-ci.org/alpha-unito/PiCo)
[![License: LGPL v3](https://img.shields.io/badge/License-LGPL%20v3-blue.svg)](https://www.gnu.org/licenses/lgpl-3.0)
[![GitHub tag](https://img.shields.io/github/tag/alpha-unito/pico.svg)](http://github.com/alpha-unito/pico/releases)
[![GitHub Issues](https://img.shields.io/github/issues/alpha-unito/pico.svg)](http://github.com/alpha-unito/pico/issues)

PiCo (**Pi**peline **Co**mposition) is an open-source C++11 header-only DSL for *high-performance data analytics*, featuring low latency, high throughput,  and minimal memory footprint on multi-core platforms.

Take a look to the [word-count](examples/word-count/pico_wc.cpp) code to see how easy is writing a PiCo pipeline!

## Report bugs + get help
https://github.com/alpha-unito/pico/issues/new

## Build and run tests
The following steps require `cmake >=3.1` as build system.

Get PiCo:
```bash
git clone https://github.com/alpha-unito/PiCo.git pico
```
The current implementation is based on [FastFlow](https://github.com/fastflow/fastflow) as runtime system.
Get it and a link it:
```bash
git clone https://github.com/fastflow/fastflow.git /path/to/fastflow
ln -s /path/to/fastflow/ff pico/
```
:rescue_worker_helmet: A better solution for including FastFlow as dependency is under development!

Build tests: (and examples)
```bash
cd pico
mkdir build && cd build
cmake .. -DPICO_ENABLE_UNIT_TEST=ON
make
```
Run Tests:
```bash
cd tests
sh run_tests.sh
```
:rescue_worker_helmet: CTest integration is under development!

## Use PiCo in you code
Good news! PiCo is header-only, you do not need to build/link any library to use it in your code.
Just include PiCo headers at the beginning of your source file:
```c++
#include "pico/pico.hpp"
```
and use good ol' compiler flags to include PiCo (and FastFlow) when compiling your `app`: 
```bash
git clone https://github.com/alpha-unito/PiCo.git /path/to/pico
git clone https://github.com/fastflow/fastflow.git /path/to/fastflow
g++ -I/path/to/pico/include i/path/to/fastflow app.cc
```
:rescue_worker_helmet: A modern CMake-based solution for linking PiCo (with its dependencies) is under development!

## Examples
The [examples](examples) folder contains some proof-of-concept applications, showing the PiCo user experience:
- [word-count](examples/word-count): PiCo pipelines 101 + visualizing application graphs
- [stock-market](examples/stock-market): batch vs stream pipelines
- [page-rank](examples/page-rank): iterative pipelines


## PiCo Team
Maurizio Drocco <maurizio.drocco@pnnl.gov> (maintainer)  
Claudia Misale <c.misale@ibm.com> (creator + co-maintainer)  
Alberto Riccardo Martinelli <alberto.martinelli@edu.unito.it> (co-maintainer)

#### Contributors
Marco Aldinucci <aldinuc@di.unito.it> (boss)  
Massimo Torquati <torquati@di.unipi.it> (FastFlow maintainer)  
Guy Tremblay <tremblay.guy@uqam.ca> (DSL wizard)

## How to cite PiCo  
C. Misale, M. Drocco, G. Tremblay, A. R. Martinelli, and M. Aldinucci, "PiCo: High-Performance Data Analytics Pipelines in Modern C++," Future Generation Computer Systems, Volume 87, 2018.  
[![SHAD_DOI_badge](https://img.shields.io/badge/DOI-https%3A%2F%2Fdoi.org%2F10.1016%2Fj.future.2018.05.030-blue.svg)](https://doi.org/10.1016/j.future.2018.05.030)
[![PiCo_BibTexview](https://img.shields.io/badge/BibTex-view-blue.svg)](https://dblp.uni-trier.de/rec/bibtex/journals/fgcs/MisaleDTMA18)
[![PiCo_BibTexdownload](https://img.shields.io/badge/BibTex-download-blue.svg)](https://dblp.uni-trier.de/rec/bib2/journals/fgcs/MisaleDTMA18.bib)
[![PiCo_RISdownload](https://img.shields.io/badge/RIS-download-blue.svg)](https://dblp.uni-trier.de/rec/ris/journals/fgcs/MisaleDTMA18.ris)
