defmodule Clickhousex.HTTPClient do
  alias Clickhousex.Query
  @moduledoc false

  @codec Application.get_env(:clickhousex, :codec, Clickhousex.Codec.JSON)

  @req_headers [{"Content-Type", "text/plain"}]

  def send(query, request, base_address, timeout, nil, _password, database) do
    opts = [hackney: [pool: :clickhouse_pool], timeout: timeout, recv_timeout: timeout]
    send_p(query, request, base_address, database, opts)
  end

  def send(query, request, base_address, timeout, username, password, database) do
    opts = [hackney: [pool: :clickhouse_pool, basic_auth: {username, password}], timeout: timeout, recv_timeout: timeout]
    send_p(query, request, base_address, database, opts)
  end

  defp send_p(query, request, base_address, database, opts) do
    command = parse_command(query)

    post_body = maybe_append_format(query, request)

    http_opts =
      Keyword.put(opts, :params, %{
        database: database,
        query: IO.iodata_to_binary(request.query_string_data)
      })

    with {:ok, %{status_code: 200, body: body}} <-
           HTTPoison.post(base_address, post_body, @req_headers, http_opts),
         {:command, :selected} <- {:command, command},
         {:ok, %{column_names: column_names, rows: rows}} <- @codec.decode(body) do
      {:ok, command, column_names, rows}
    else
      {:command, :updated} -> {:ok, :updated, 1}
      {:ok, response} -> {:error, response.body}
      {:error, error} -> {:error, error.reason}
    end
  end

  defp parse_command(%Query{type: :select}), do: :selected
  defp parse_command(_), do: :updated

  defp maybe_append_format(%Query{type: :select}, request) do
    [request.post_data, " FORMAT ", @codec.response_format()]
  end

  defp maybe_append_format(_, request) do
    [request.post_data]
  end
end
