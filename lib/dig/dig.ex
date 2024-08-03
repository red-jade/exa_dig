defmodule Exa.Dig.Dig do
  @moduledoc """
  Utilities for directed graphs using the Erlang _digraph_ library.

  The graph may be:
  - _cyclic_ generalized directed graph: allow cycles and self-loops
  - _acyclic_ 'Directed Acyclic Graph' (DAG): no cycles or self-loops

  Repeated edges are not allowed. 
  There is at most one edge between the same ordered pair of vertices.

  The _digraph_ library stores vertex and edges data in ETS.
  Erlang _digraph_ and ETS store state in a separate proceess.
  So the graph object is stateful, it does not need to be
  threaded through all function calls.
  However, it does need to be destroyed to free resources
  if the client process is finished with the graph.
  """
  import Exa.Types
  alias Exa.Types, as: E

  # TODO - convert DOT types to agr when available
  use Exa.Dot.Constants
  import Exa.Dot.Types
  alias Exa.Dot.Types, as: D

  alias Exa.Dot.DotReader
  alias Exa.Dot.DotWriter, as: DOT

  alias Exa.Std.HistoTypes, as: H
  alias Exa.Std.Histo1D
  alias Exa.Std.Histo2D

  # -----
  # types
  # -----

  # dialyzer does not like this violation of opaque types

  @doc "Test if a term is an Erlang digraph."
  defguard is_dig(g) when is_tuple(g) and elem(g, 0) == :digraph

  @typedoc """
  Degree is the count of edges for a vertex.

  Depending on the adjacency type:
  - `:in`: number of incoming edges (for dst vertex)
  - `:out`: number of outgoing edges (for src vertex)
  - `:inout`: combined degree in+out for all incident edges
  """
  @type degree() :: E.count()

  @typedoc """
  Type of adjacency neighborhood:
  - `in` incoming edges and upstream neighbors
  - `out` outgoing edges and downstream neighbors
  - `inout` combined incident edges and all adjacent neighbors
  """
  @type adjacency() :: :in | :out | :inout

  @typedoc """
  Cyclicity property for the whole graph:
  The boolean argument is for cyclicity:
  - `:cyclic` general directed graph, allow cycles and self-loops
  - `:acyclic` Directed Acyclic Graph (DAG), no cycles or self-loops
  """
  @type cyclicity() :: :cyclic | :acyclic

  # -------------------
  # graph create/delete
  # -------------------

  @doc "Create a new empty graph."
  @spec new(cyclicity()) :: :digraph.graph()
  def new(cyc \\ :cyclic) when cyc in [:cyclic, :acyclic], do: :digraph.new([cyc])

  @doc """
  Create a new graph containing the specified vertices and edges.

  Edge creation forces creation of missing vertices.
  """
  @spec new(D.verts(), D.edges(), cyclicity()) :: :digraph.graph()
  def new(verts, edges, cyc \\ :cyclic) when is_list(verts) and is_list(edges) do
    g = new(cyc)
    Enum.each(verts, fn i when is_vert(i) -> add_vert(g, i) end)
    Enum.each(edges, fn {i, j} = e when is_edge(e) -> add_edge(g, i, j, true) end)
    g
  end

  @doc "Delete the graph."
  @spec delete(:digraph.graph()) :: true
  def delete(dig), do: :digraph.delete(dig)

  # -------------
  # graph queries
  # -------------

  @doc "The number of vertices in the graph."
  @spec nvert(:digraph.graph()) :: E.count()
  def nvert(g) when is_dig(g), do: :digraph.no_vertices(g)

  @doc "The number of edges in the graph."
  @spec nedge(:digraph.graph()) :: E.count()
  def nedge(g) when is_dig(g), do: :digraph.no_edges(g)

  @doc "Get the digraph vertices as a list of ids."
  @spec verts(:digraph.graph()) :: D.verts()
  def verts(g) when is_dig(g), do: g |> :digraph.vertices() |> vids()

  @doc "Get the digraph edges as a list of directed edges."
  @spec edges(:digraph.graph()) :: D.edges()
  def edges(g) when is_dig(g), do: g |> :digraph.edges() |> eids(g)

  @doc "Test if the vertex exists in the graph."
  @spec vert?(:digraph.graph(), D.vert()) :: bool()
  def vert?(g, i) when is_dig(g) and is_vert(i) do
    # yes, should use !! here, but this is clearer
    case :digraph.vertex(g, vmake(i)) do
      false -> false
      _ -> true
    end
  end

  @doc "Test if the edge exists in the graph."
  @spec edge?(:digraph.graph(), D.edge()) :: bool()
  def edge?(g, {i, j} = e) when is_dig(g) and is_edge(e) do
    vmake(j) in :digraph.out_neighbours(g, vmake(i))
  end

  @doc """
  Get the degree for a vertex, given an adjacency relationship.

  Returns an error if the vertex does not exist.
  """
  @spec degree(:digraph.graph(), D.vert(), adjacency()) ::
          {D.vert(), in_or_out :: degree()}
          | {D.vert(), n_in :: degree(), n_out :: degree()}
          | {:error, any()}

  def degree(g, i, :in) when is_dig(g) and is_vert(i) do
    if vert?(g, i) do
      iv = vmake(i)
      {i, :digraph.in_degree(g, iv)}
    else
      {:error, "Missing vertex #{i}"}
    end
  end

  def degree(g, i, :out) when is_dig(g) and is_vert(i) do
    if vert?(g, i) do
      iv = vmake(i)
      {i, :digraph.out_degree(g, iv)}
    else
      {:error, "Missing vertex #{i}"}
    end
  end

  def degree(g, i, :inout) when is_dig(g) and is_vert(i) do
    if vert?(g, i) do
      iv = vmake(i)
      {i, :digraph.in_degree(g, iv), :digraph.out_degree(g, iv)}
    else
      {:error, "Missing vertex #{i}"}
    end
  end

  @doc """
  Get the neighbors of a vertex, given an adjacency relationship.

  Returns an error if the vertex does not exist.
  """
  @spec neighborhood(:digraph.graph(), D.vert(), adjacency()) ::
          {D.vert(), in_or_out :: D.verts()}
          | {D.vert(), v_in :: D.verts(), v_out :: D.verts()}
          | {:error, any()}

  def neighborhood(g, i, :in) when is_dig(g) and is_vert(i) do
    if vert?(g, i) do
      {i, vids(:digraph.in_neighbours(g, vmake(i)))}
    else
      {:error, "Missing vertex #{i}"}
    end
  end

  def neighborhood(g, i, :out) when is_dig(g) and is_vert(i) do
    if vert?(g, i) do
      {i, vids(:digraph.out_neighbours(g, vmake(i)))}
    else
      {:error, "Missing vertex #{i}"}
    end
  end

  def neighborhood(g, i, :inout) when is_dig(g) and is_vert(i) do
    if vert?(g, i) do
      iv = vmake(i)
      {i, vids(:digraph.in_neighbours(g, iv)), vids(:digraph.out_neighbours(g, iv))}
    else
      {:error, "Missing vertex #{i}"}
    end
  end

  # -------------------
  # vertex/edge add/del
  # -------------------

  @doc """
  Add a vertex to the graph.

  It is not an error to add the same vertex ID more than once.
  """
  @spec add_vert(:digraph.graph(), D.vert()) :: D.vert()
  def add_vert(g, i) when is_dig(g) and is_vert(i) do
    [~c"$v" | ^i] = :digraph.add_vertex(g, vmake(i))
    i
  end

  @doc """
  Add a list of vertices to the graph.

  See `add_vert/2`.
  """
  @spec add_verts(digraph_graph :: tuple(), D.verts()) :: :ok | {:error, any()}
  def add_verts(g, verts) when is_dig(g) and is_list(verts) do
    Enum.reduce_while(verts, :ok, fn i, :ok ->
      case add_vert(g, i) do
        ^i -> {:cont, :ok}
        {:error, _} = err -> {:halt, err}
      end
    end)
  end

  @doc """
  Add a directed edge to the graph.

  If the `create?` flag is `true` (default), 
  then create any vertices that do not already exist.

  If the `create?` flag is `false`,
  and a vertex does not exist, then return error.

  If the graph was created _acyclic_ 
  then self-loops and cyclic paths will force an error.

  If the graph is _cyclic_ then self-loops are allowed.

  Repeated edges are not allowed.
  There can be at most one edge with the same ordered pair of endpoints.
  """
  @spec add_edge(:digraph.graph(), D.vert(), D.vert(), bool()) :: D.edge() | {:error, any()}
  def add_edge(g, i, j, create? \\ true) when is_dig(g) and is_vert(i) and is_vert(j) do
    if edge?(g, {i, j}) do
      {:error, "Existing edge {#{i},#{j}}"}
    else
      case :digraph.add_edge(g, vmake(i), vmake(j)) do
        [:"$e" | _eid] ->
          {i, j}

        {:error, {:bad_vertex, [~c"$v" | k]}} when create? ->
          add_vert(g, k)
          add_edge(g, i, j, create?)

        {:error, {:bad_vertex, [~c"$v" | k]}} ->
          {:error, "Missing vertex #{k}"}

        {:error, {:bad_edge, path}} ->
          {:error, "Cyclic path #{inspect(vids(path), charlists: :as_lists)}"}
      end
    end
  end

  @doc """
  Add a list of directed edges to the graph.

  See `add_edge/4`.
  """
  @spec add_edges(digraph_graph :: tuple(), D.edges(), bool()) :: :ok | {:error, any()}
  def add_edges(g, edges, create? \\ true) when is_dig(g) and is_list(edges) do
    Enum.reduce_while(edges, :ok, fn {i, j}, :ok ->
      case add_edge(g, i, j, create?) do
        {^i, ^j} -> {:cont, :ok}
        {:error, _} = err -> {:halt, err}
      end
    end)
  end

  @doc """
  Delete a vertex from the graph.

  All incident edges are also deleted.

  It is not an error to delete a non-existent vertex.
  """
  @spec del_vert(:digraph.graph(), D.vert()) :: :ok | {:error, any()}
  def del_vert(g, i) when is_dig(g) and is_vert(i) do
    if vert?(g, i) do
      :digraph.del_vertex(g, vmake(i))
      :ok
    else
      {:error, "Missing vertex #{i}"}
    end
  end

  @doc """
  Delete an edge from the graph.
  """
  @spec del_edge(:digraph.graph(), D.edge()) :: :ok | {:error, any()}
  def del_edge(g, e) when is_dig(g) and is_edge(e) do
    case efind(g, e) do
      nil -> {:error, "Missing edge #{e}"}
      edig -> :digraph.del_edge(g, edig)
    end
  end

  # -----------
  # conversions
  # -----------

  @doc """
  Write a digraph to file in GraphViz DOT format.

  The graph `gname` is used as the title of the DOT graph object,
  as a key for global properties in the graph attribute map,
  and as the basename for the output file.

  Return the DOT text as `chardata` and the full output filename.

  Use `Exa.Dot.Render.render_dot/3` 
  to convert the DOT file to PNG or SVG.
  """
  @spec to_dot_file(:digraph.graph(), E.filename(), D.gname(), D.graph_attrs()) ::
          {E.filename(), IO.chardata()}
  def to_dot_file(g, dotdir, gname, gattrs \\ %{})
      when is_dig(g) and is_filename(dotdir) and is_string(gname) do
    Exa.File.ensure_dir!(dotdir)
    filename = Exa.File.join(dotdir, gname, [@filetype_dot])

    dot =
      DOT.new_dot(gname)
      |> DOT.globals(gname, gattrs)
      |> DOT.nodes(verts(g), gattrs)
      |> DOT.edges(edges(g), gattrs)
      |> DOT.end_dot()
      |> DOT.to_file(filename)

    {filename, dot}
  end

  @doc """
  Read DOT file into digraph.
  """
  @spec from_dot_file(E.filename()) :: {:digraph.graph(), D.graph_attrs()}
  def from_dot_file(filename) when is_filename(filename) do
    {agr, gattrs} = DotReader.from_dot(filename)
    g = new()
    Enum.each(agr, fn
      {i,j} = e when is_edge(e) -> add_edge(g,i,j, true)
      i when is_vert(i) -> add_vert(g,i)
    end)
    {g, gattrs}
  end

  # ----------
  # histograms
  # ----------

  @doc """
  Create a 1D histogram of the vertex degrees.

  The kind of degree is determined by the adjacency argument:
  - `:in` in degree
  - `:out` out degree
  - `:inout` total degree (in+out)
  """
  @spec degree_histo1d(:digraph.graph(), adjacency()) :: H.histo1d()

  def degree_histo1d(g, :in) do
    Enum.reduce(verts(g), Histo1D.new(), fn i, h ->
      Histo1D.inc(h, :digraph.in_degree(g, vmake(i)))
    end)
  end

  def degree_histo1d(g, :out) do
    Enum.reduce(verts(g), Histo1D.new(), fn i, h ->
      Histo1D.inc(h, :digraph.out_degree(g, vmake(i)))
    end)
  end

  def degree_histo1d(g, :inout) do
    Enum.reduce(verts(g), Histo1D.new(), fn i, h ->
      iv = vmake(i)
      deg = :digraph.in_degree(g, iv) + :digraph.out_degree(g, iv)
      Histo1D.inc(h, deg)
    end)
  end

  @doc """
  Create a 2D histogram of the in and out vertex degrees.
  """
  @spec degree_histo2d(:digraph.graph()) :: H.histo2d()
  def degree_histo2d(g) do
    Enum.reduce(verts(g), Histo2D.new(), fn i, h ->
      iv = vmake(i)
      indeg = :digraph.in_degree(g, iv)
      outdeg = :digraph.out_degree(g, iv)
      Histo2D.inc(h, {indeg, outdeg})
    end)
  end

  @doc """
  Create a hash of the graph.

  The hash should reasonably discriminate graphs
  by their topology with a simple and relatively fast algorithm.

  The hash can be used to reject a graph isomorphism test.
  Graphs with different hashes cannot be isomorphic.
  Graphs with the same hash may be isomorphic, or not,
  they are _undecided._

  The current approach is to generate the 2D histogram,
  serialize to a list, sort, convert term to binary,
  then hash using SHA-256.

  The algorithm is not guaranteed to be stable over time
  or between different Erlang runtime instances.
  The hash should not be persisted.
  """
  @spec hash(:digraph.graph()) :: binary()
  def hash(g) when is_dig(g) do
    bin = g |> degree_histo2d() |> Enum.sort() |> :erlang.term_to_binary([:local])
    :crypto.hash(:sha256, bin)
  end

  @doc "Test two graphs for exact equality."
  @spec equal?(:digraph.graph(), :digraph.graph()) :: bool()
  def equal?(g1, g2) do
    case isomorphic?(g1, g2) do
      false ->
        false

      :undecided ->
        g1 |> verts() |> Enum.sort() == g2 |> verts() |> Enum.sort() and
          g1 |> edges() |> Enum.sort() == g2 |> edges() |> Enum.sort()
    end
  end

  @doc """
  Test two graphs for isomorphism.

  The result is either `true`, `false` or `:undecided`.

  The test compares number of vertices, then number of edges,
  then hashes of the graphs 
  (currently based on the sorted 2D histogram of vertex degrees).

  Does not do a full equality check.
  """
  @spec isomorphic?(:digraph.graph(), :digraph.graph()) :: bool() | :undecided
  def isomorphic?(g1, g2) when is_dig(g1) and is_dig(g2) do
    if nvert(g1) == nvert(g2) and nedge(g1) == nedge(g2) and hash(g1) == hash(g2) do
      # don't tets for equality here
      # keep equality check separate
      :undecided
    else
      false
    end
  end

  # -----------------
  # private functions
  # -----------------

  # convert digraph edges to dig vertex pairs
  @dialyzer {:no_unused, eids: 2}
  @spec eids([:digraph.edge()], :digraph.graph()) :: D.edges()
  defp eids(es, g) when is_list(es) do
    Enum.map(es, fn e ->
      {_id, v1, v2, _label} = :digraph.edge(g, e)
      {vid(v1), vid(v2)}
    end)
  end

  # extract id from a list of vertices
  @dialyzer {:no_unused, vids: 1}
  @spec vids([:digraph.vertex()]) :: D.verts()
  defp vids(vs) when is_list(vs), do: Enum.map(vs, &vid/1)

  # extract the id from a dig vertex
  @spec vid(:digraph.vertex()) :: D.vert()
  defp vid([~c"$v" | i]) when is_vert(i), do: i

  # find an existing edge, when the whole edge record is needed
  @spec efind(:digraph.graph(), D.edge()) :: nil | :digraph.edge()
  def efind(g, {i, j}) do
    jv = vmake(j)

    Enum.find(:digraph.out_edges(g, vmake(i)), fn eid ->
      {_eid, _iv, kv, _label} = :digraph.edge(g, eid)
      kv == jv
    end)
  end

  # create a dig vertex 
  @spec vmake(D.vert()) :: :digraph.vertex()
  defp vmake(i) when is_vert(i), do: [~c"$v" | i]
end
