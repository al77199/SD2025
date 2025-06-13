using Grpc.Core;
using PreProcess;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json;

/*public class PreProcessServiceImpl : PreProcessService.PreProcessServiceBase
{

    public override Task<LoteResponse> ProcessarLote(LoteRequest request, ServerCallContext ctx)
    {
        if (request.LeiturasJson.Count == 0 || request.WavyId == "PING")
        {
            Console.WriteLine($"[PROC] Heartbeat recebido ({request.WavyId})");
            return Task.FromResult(new LoteResponse
            {
                Sucesso = true,        // ligação OK
                Csv = Google.Protobuf.ByteString.Empty
            });
        }


        var sw = Stopwatch.StartNew();
        string reqId = Guid.NewGuid().ToString("N")[..8];      // mini-UID p/ logs

        Console.ForegroundColor = ConsoleColor.Cyan;
        Console.WriteLine($"[PROC {reqId}] Pedido de {request.WavyId}  " +
                          $"({request.LeiturasJson.Count} leituras)");
        Console.ResetColor();

        var csv = new StringBuilder();
        csv.AppendLine("wavyId;timestamp;rawJson");

        int erros = 0;
        foreach (var leitura in request.LeiturasJson)
        {
            try
            {
                using var doc = JsonDocument.Parse(leitura);
                string ts = doc.RootElement.TryGetProperty("timestamp", out var t) ? t.GetString() ?? "" : "";
                //csv.AppendLine($"{request.WavyId},{ts},\"{leitura.Replace("\"", "\"\"")}\"");
                double valor = root.GetProperty("Temperatura").GetDouble();
                csv.AppendLine($"{wavyId};{valor.ToString(CultureInfo.InvariantCulture)};{ts}");

            }
            catch (Exception ex)
            {
                erros++;
                Console.WriteLine($"[PROC {reqId}] JSON inválido: {ex.Message}");
            }
        }

        var csvBytes = Google.Protobuf.ByteString.CopyFromUtf8(csv.ToString());
        sw.Stop();

        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine($"[PROC {reqId}]  CSV gerado ({csvBytes.Length} bytes)  " +
                          $"– erros parsing: {erros} – tempo: {sw.ElapsedMilliseconds} ms");
        Console.WriteLine($"[PROC {reqId}]  Resposta enviada\n");
        Console.ResetColor();

        var resp = new LoteResponse
        {
            Sucesso = true,
            Csv = csvBytes
        };
        return Task.FromResult(resp);
    }
}*/


using System;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Grpc.Core;
using PreProcess;  // namespace gerado a partir de preprocessor.proto

public class PreProcessServiceImpl : PreProcessService.PreProcessServiceBase
{
    public override Task<LoteResponse> ProcessarLote(LoteRequest request, ServerCallContext ctx)
    {
        // 1) Heartbeat ou lote vazio
        if (request.LeiturasJson.Count == 0 || request.WavyId == "PING")
        {
            Console.WriteLine($"[PROC] Heartbeat recebido ({request.WavyId})");
            return Task.FromResult(new LoteResponse
            {
                Sucesso = true,
                Csv = Google.Protobuf.ByteString.Empty
            });
        }

        var sw = Stopwatch.StartNew();
        var reqId = Guid.NewGuid().ToString("N")[..8];  // UID curto para logs

        Console.ForegroundColor = ConsoleColor.Cyan;
        Console.WriteLine($"[PROC {reqId}] Pedido de {request.WavyId}  ({request.LeiturasJson.Count} leituras)");
        Console.ResetColor();

        // 2) Cabeçalho CSV: wavyId;value;timestamp
        var sb = new StringBuilder();
        sb.AppendLine("wavyId;value;timestamp");

        int erros = 0;

        foreach (var rawJson in request.LeiturasJson)
        {
            try
            {
                using var doc = JsonDocument.Parse(rawJson);
                var root = doc.RootElement;

                // 2a) Extrai timestamp (aceita "Timestamp" ou "timestamp")
                string ts = "";
                if (root.TryGetProperty("Timestamp", out var t1))
                    ts = t1.GetString() ?? "";
                else if (root.TryGetProperty("timestamp", out var t2))
                    ts = t2.GetString() ?? "";

                // 2b) Extrai valor numérico do sensor
                //    (Temperatura, Rain ou Wind conforme o WavyId)
                double valor;
                if (root.TryGetProperty("Temperatura", out var pT))
                    valor = pT.GetDouble();
                else if (root.TryGetProperty("Rain", out var pR))
                    valor = pR.GetDouble();
                else if (root.TryGetProperty("Wind", out var pW))
                    valor = pW.GetDouble();
                else
                    throw new JsonException("Campo de sensor não encontrado");

                // 2c) Gera linha CSV
                sb.AppendLine(
                    $"{request.WavyId};" +
                    $"{valor.ToString(CultureInfo.InvariantCulture)};" +
                    $"{ts}"
                );
            }
            catch (Exception ex)
            {
                erros++;
                Console.WriteLine($"[PROC {reqId}] JSON inválido: {ex.Message}");
            }
        }

        // 3) Converte para ByteString e loga
        var csvText = sb.ToString();
        var csvBytes = Google.Protobuf.ByteString.CopyFromUtf8(csvText);
        sw.Stop();

        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine($"[PROC {reqId}] CSV gerado ({csvBytes.Length} bytes) – erros: {erros} – tempo: {sw.ElapsedMilliseconds} ms");
        Console.WriteLine($"[PROC {reqId}] Resposta enviada\n");
        Console.ResetColor();

        // 4) Retorna resposta
        return Task.FromResult(new LoteResponse
        {
            Sucesso = (erros == 0),
            Csv = csvBytes
        });
    }
}


/*
public class PreProcessServiceImpl : PreProcessService.PreProcessServiceBase
{
    public override Task<LoteResponse> ProcessarLote(LoteRequest request, ServerCallContext ctx)
    {
        // Heartbeat / lote vazio
        if (request.LeiturasJson.Count == 0 || request.WavyId == "PING")
        {
            Console.WriteLine($"[PROC] Heartbeat recebido ({request.WavyId})");
            return Task.FromResult(new LoteResponse
            {
                Sucesso = true,
                Csv = Google.Protobuf.ByteString.Empty
            });
        }

        var sb = new StringBuilder();
        sb.AppendLine("wavyId;value;timestamp");

        int erros = 0;
        foreach (var rawJson in request.LeiturasJson)
        {
            try
            {
                using var doc = JsonDocument.Parse(rawJson);
                var root = doc.RootElement;

                // 1) Extrai timestamp
                string ts = "";
                if (root.TryGetProperty("Timestamp", out var tProp) ||
                    root.TryGetProperty("timestamp", out tProp))
                {
                    ts = tProp.GetString() ?? "";
                }

                // 2) Descobre qual o campo do sensor e extrai o valor
                double valor;
                if (root.TryGetProperty("Temperatura", out var numProp) ||
                    root.TryGetProperty("Rain", out numProp) ||
                    root.TryGetProperty("Wind", out numProp))
                {
                    valor = numProp.GetDouble();
                }
                else
                {
                    // nenhum campo encontrado
                    throw new JsonException("Campo de sensor não encontrado");
                }

                // 3) Monta linha CSV
                sb.AppendLine($"{request.WavyId};" +
                              $"{valor.ToString(CultureInfo.InvariantCulture)};" +
                              $"{ts}");
            }
            catch (Exception ex)
            {
                erros++;
                Console.WriteLine($"[PROC] JSON inválido/lido: {ex.Message}");
            }
        }

        var csvText = sb.ToString();
        var csvBytes = Google.Protobuf.ByteString.CopyFromUtf8(csvText);

        Console.WriteLine($"[PROC] {request.WavyId}: CSV com {request.LeiturasJson.Count - erros} linhas, " +
                          $"{erros} erros");

        return Task.FromResult(new LoteResponse
        {
            Sucesso = (erros == 0),
            Csv = csvBytes
        });
    }
}
*/

