defmodule Clickhousex.Codec.Values do
  alias Clickhousex.Query

  def encode(%Query{param_count: 0, type: :insert}, _, []) do
    # An insert query's arguments go into the post body and the query part goes into the query string.
    # If we don't have any arguments, we don't have to encode anything, but we don't want to return
    # anything here because we'll duplicate the query into both the query string and post body
    ""
  end

  def encode(%Query{param_count: 0, statement: statement}, _, []) do
    statement
  end

  def encode(%Query{param_count: 0}, _, _) do
    raise ArgumentError, "Extra params! Query doesn't contain '?'"
  end

  def encode(%Query{param_count: param_count} = query, query_text, params) do
    if length(params) != param_count do
      raise ArgumentError,
            "The number of parameters does not correspond to the number of question marks!"
    end

    query_parts = String.split(query_text, "?")

    weave(query, query_parts, params)
  end

  defp weave(query, query_parts, params) do
    weave(query, query_parts, params, [])
  end

  defp weave(_query, [part], [], acc) do
    Enum.reverse([part | acc])
  end

  defp weave(query, [part | parts], [param | params], acc) do
    weave(query, parts, params, [encode_param(query, param), part | acc])
  end

  @doc false
  defp encode_param(query, param) when is_list(param) do
    values = Enum.map_join(param, ",", &encode_param(query, &1))

    case query.type do
      :select ->
        # We pass lists to in clauses, and they shouldn't have brackets around them.
        values

      _ ->
        ["[", values, "]"]
    end
  end

  defp encode_param(_query, param) when is_integer(param) do
    Integer.to_string(param)
  end

  defp encode_param(_query, true) do
    "1"
  end

  defp encode_param(_query, false) do
    "0"
  end

  defp encode_param(_query, param) when is_float(param) do
    to_string(param)
  end

  defp encode_param(_query, param) when is_float(param) do
    to_string(param)
  end

  defp encode_param(_query, nil) do
    "NULL"
  end

  defp encode_param(_query, %DateTime{} = datetime) do
    iso_date =
      datetime
      |> DateTime.truncate(:second)
      |> DateTime.to_iso8601()
      |> String.replace("Z", "")

    ["'", iso_date, "'"]
  end

  defp encode_param(_query, %NaiveDateTime{} = naive_datetime) do
    naive =
      naive_datetime
      |> NaiveDateTime.truncate(:second)
      |> NaiveDateTime.to_iso8601()

    ["'", naive, "'"]
  end

  defp encode_param(_query, %Date{} = date) do
    ["'", Date.to_iso8601(date), "'"]
  end

  defp encode_param(_query, param) do
    ["'", escape(param), "'"]
  end

  defp escape(s) when is_map(s) do
    m = Jason.encode!(s)
    to_iodata(m, 0, m, [])
  end

  defp escape(s) do
    to_iodata(s, 0, s, [])
  end

  ### ESCAPING logic is based on https://github.com/elixir-plug/plug/blob/master/lib/plug/html.ex#L41-L80. Probably the fastest way to escape strings in Elixir
  escapes = [
    {?_, "\_"},
    {?', "''"},
    {?%, "\%"},
    {?\\, "\\\\"}
  ]

  for {match, insert} <- escapes do
    defp to_iodata(<<unquote(match), rest::bits>>, skip, original, acc) do
      to_iodata(rest, skip + 1, original, [acc | unquote(insert)])
    end
  end

  defp to_iodata(<<_char, rest::bits>>, skip, original, acc) do
    to_iodata(rest, skip, original, acc, 1)
  end

  defp to_iodata(<<>>, _skip, _original, acc) do
    acc
  end

  for {match, insert} <- escapes do
    defp to_iodata(<<unquote(match), rest::bits>>, skip, original, acc, len) do
      part = binary_part(original, skip, len)
      to_iodata(rest, skip + len + 1, original, [acc, part | unquote(insert)])
    end
  end

  defp to_iodata(<<_char, rest::bits>>, skip, original, acc, len) do
    to_iodata(rest, skip, original, acc, len + 1)
  end

  defp to_iodata(<<>>, 0, original, _acc, _len) do
    original
  end

  defp to_iodata(<<>>, skip, original, acc, len) do
    [acc | binary_part(original, skip, len)]
  end
end
