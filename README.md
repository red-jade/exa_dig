## EXA Dig

𝔼𝕏tr𝔸 𝔼li𝕏ir 𝔸dditions (𝔼𝕏𝔸)

EXA project index: [exa](https://github.com/red-jade/exa)

Utilities for directed graphs using the Erlang _digraph_ library.

Module path: `Exa.Dig`

### Features

Wrapper around Erlang `digraph` module.

Graphs allow cyclic graphs and self-loops, but not multi-edges 
(multiple edges between the same pair of vertices).

Functions to fetch vertex degrees and neighborhoods.
Build 1D and 2D histograms from vertex degrees,
and hence generate a hash for a graph.
Use the hash for simple isomorphism test.

Conversion to and from GraphViz DOT file format.

### License

EXA source code is released under the MIT license.

EXA code and documentation are:<br>
Copyright (c) 2024 Mike French
