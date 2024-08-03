defmodule Exa.Dig.Build do
  @moduledoc """
  Utilities for building directed graphs using the Erlang _digraph_ library.
  """

  import Exa.Types
  alias Exa.Types, as: E

  alias Exa.Dig.Dig
  alias Exa.Random

  # ------------
  # constructors
  # ------------

  @doc "Disconnected dust topology."
  @spec dust(E.count1()) :: :digraph.graph()
  def dust(n) when is_count1(n) do
    g = Dig.new()
    Enum.each(1..n, fn i -> Dig.add_vert(g, i) end)
    g
  end

  @doc "Simple directed line topology."
  @spec line(E.count1()) :: :digraph.graph()
  def line(n) when is_count1(n) do
    g = Dig.new()
    Dig.add_vert(g, 1)

    for i <- 2..n do
      Dig.add_vert(g, i)
      Dig.add_edge(g, i - 1, i)
    end

    g
  end

  @doc "Simple ring topology."
  @spec ring(E.count1()) :: :digraph.graph()
  def ring(n) when is_count1(n) do
    g = line(n)
    Dig.add_edge(g, n, 1)
    g
  end

  @doc "Star topology directed outwards."
  @spec fan_out(E.count1()) :: :digraph.graph()
  def fan_out(n) when is_integer(n) and n > 2 do
    g = Dig.new()
    Dig.add_vert(g, 1)

    for i <- 2..n do
      Dig.add_vert(g, i)
      Dig.add_edge(g, 1, i)
    end

    g
  end

  @doc "Star topology directed inwards."
  @spec fan_in(E.count1()) :: :digraph.graph()
  def fan_in(n) when is_integer(n) and n > 2 do
    g = Dig.new()
    Dig.add_vert(g, 1)

    for i <- 2..n do
      Dig.add_vert(g, i)
      Dig.add_edge(g, i, 1)
    end

    g
  end

  @doc "Wheel topology directed outwards."
  @spec wheel(E.count1()) :: :digraph.graph()
  def wheel(n) when is_integer(n) and n > 3 do
    g = fan_out(n)
    for i <- 3..n, do: Dig.add_edge(g, i - 1, i)
    Dig.add_edge(g, n, 2)
    g
  end

  @doc "Clique fully connected in both directions."
  @spec clique(E.count1()) :: :digraph.graph()
  def clique(n) when is_count1(n) do
    g = Dig.new()

    for i <- 1..n, do: Dig.add_vert(g, i)

    for i <- 1..n do
      for j <- 1..n, j != i, do: Dig.add_edge(g, i, j)
    end

    g
  end

  @doc "Create a regular 2D lattice with undirected (bidirectional) edges."
  @spec grid2d(E.count1(), E.count1()) :: :digraph.graph()
  def grid2d(nx, ny) when is_count1(nx) and is_count1(ny) do
    g = Dig.new()

    for i <- 1..(nx * ny), do: Dig.add_vert(g, i)

    Enum.reduce(1..ny, 1, fn _y, u ->
      Enum.reduce(1..(nx - 1), u, fn _x, u ->
        v = u + 1
        Dig.add_edge(g, u, v)
        Dig.add_edge(g, v, u)
        v
      end) + 1
    end)

    Enum.reduce(1..(ny - 1), 1, fn _y, u ->
      Enum.reduce(1..nx, u, fn _x, u ->
        v = u + nx
        Dig.add_edge(g, u, v)
        Dig.add_edge(g, v, u)
        u + 1
      end)
    end)

    g
  end

  @doc """
  Random graph.

  The number of edges, m, must be greater 
  than the number of vertices, n, for the graph to be connected: m > n.

  The number of edges should not be close to a complete graph: m << n^2
  """
  @spec random(E.count1(), E.count1(), bool()) :: :digraph.graph()
  def random(n, m, force_connected \\ true)
      when is_integer(n) and n > 1 and
             is_integer(m) and m > n and
             m < 2 * n * (n - 1) do
    g = Dig.new()
    random_verts(g, n)
    random_edges(g, n, m, force_connected)
    g
  end

  # -----------------
  # private functions
  # -----------------

  # TODO - random construction is a quick hack ...

  # Create N vertices.
  @spec random_verts(:digraph.graph(), E.count1()) :: :ok
  defp random_verts(g, n) do
    Enum.each(1..n, fn i -> Dig.add_vert(g, i) end)
    ^n = Dig.nvert(g)
    :ok
  end

  # Create M random edges avoiding 
  # self-loops and repeated connections.
  # Will be very inefficiient approaching a complete graph m -> n(n-1)/2,
  # so best when m << n^2
  @spec random_edges(:digraph.graph(), E.count1(), E.count1(), bool()) :: :ok

  # TODO - expand the bool to be an atom of connection choices:
  # connected (1 conn comp)
  # not connected (1+ conn comp)
  # k components  (k <= n)

  defp random_edges(g, n, m, force_connected)

  defp random_edges(g, n, m, false) do
    erandom(g, n, m, 0)
  end

  defp random_edges(g, n, m, true) do
    # force every vertex to have at least one edge
    # to ensure that the whole graph is (weakly) connected
    Enum.each(1..n, fn i -> Dig.add_edge(g, i, Random.rndint(n, i)) end)

    # add remaining edges randomly
    erandom(g, n, m, n)
  end

  defp erandom(g, n, m, m) do
    # assert result   
    ^n = Dig.nvert(g)
    :ok
  end

  defp erandom(g, n, m, k) when k < m do
    i = Random.rndint(n)
    j = Random.rndint(n, i)

    if Dig.edge?(g, {i, j}) do
      erandom(g, n, m, k)
    else
      Dig.add_edge(g, i, j)
      erandom(g, n, m, k + 1)
    end
  end
end
