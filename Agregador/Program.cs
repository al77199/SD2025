// AggregatorApp – consome RabbitMQ, agrupa em lotes e envia ao PreProcessServer via gRPC
// .NET 7 – pacotes NuGet: Grpc.Net.Client, Google.Protobuf, Grpc.Tools, RabbitMQ.Client
using Grpc.Net.Client;
using PreProcess;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Net;


namespace AggregatorApp;

internal class Program
{
    //------------TCP-------------
    private const string SERVER_IP = "127.0.0.1";
    private const int SERVER_PORT = 6000;
    private const int HEARTBEAT_MS = 10_000;
    private static TcpClient serverClient;
    private static StreamReader srServer;
    private static StreamWriter swServer;
    private static bool serverConnected = false;
    private static readonly object serverLock = new();

    // ---------- gRPC (canal único reutilizado) ----------
    private static readonly GrpcChannel GrpcChannel =
        GrpcChannel.ForAddress("http://localhost:5100");
    private static readonly PreProcessService.PreProcessServiceClient GrpcClient =
        new(GrpcChannel);

    // ---------- batch ----------
    private static readonly TimeSpan FLUSH_INTERVAL = TimeSpan.FromMinutes(1);
    private static readonly Dictionary<string, List<string>> batchBuffers = new();
    private static readonly Dictionary<string, DateTime> lastFlush = new();
    private static readonly object batchLock = new();

    // ----------------------------------------------------
    private static async Task Main()
    {
        Console.WriteLine("[AGG] Arrancar…");

        ConnectToServer();
        new Thread(HeartbeatLoop) { IsBackground = true }.Start();

        // Teste rápido à ligação gRPC (envia lote vazio)
        try
        {
            await GrpcClient.ProcessarLoteAsync(new LoteRequest { WavyId = "PING" });
            Console.WriteLine("[AGG]  Ligação gRPC OK (localhost:5001)");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[AGG]  Falha inicial gRPC: {ex.Message}");
        }

        // ▶️ Arrancar consumidor RabbitMQ
        _ = StartRabbitConsumerAsync();

        Console.WriteLine("[AGG] Pronto. Carrega ENTER para sair.");
        Console.ReadLine();
    }
    //------------------------------Ligacao ao servidor----------------------------------------
    private static void ConnectToServer()
    {
        lock (serverLock)
        {
            if (serverConnected) return;

            try { serverClient?.Close(); } catch { }

            try
            {
                serverClient = new TcpClient();
                serverClient.Connect(SERVER_IP, SERVER_PORT);

                var ns = serverClient.GetStream();
                srServer = new StreamReader(ns);
                swServer = new StreamWriter(ns) { AutoFlush = true };

                swServer.WriteLine("PEDIDO_CONEXAO;AGG=AGG1");
                string resp = srServer.ReadLine();
                serverConnected = resp != null && resp.Contains("OK_CONEXAO");
                Console.WriteLine(serverConnected
                    ? "[AGG] Conectado ao Servidor."
                    : "[AGG] Handshake falhou.");
            }
            catch (Exception ex)
            {
                Console.WriteLine("[AGG] Erro ligar Servidor: " + ex.Message);
                serverConnected = false;
            }
        }
    }

    private static void HeartbeatLoop()
    {
        while (true)
        {
            Thread.Sleep(HEARTBEAT_MS);

            lock (serverLock)
            {
                if (!serverConnected)
                {
                    ConnectToServer();
                }
                else
                {
                    try
                    {
                        swServer.WriteLine("PING");
                        var pong = srServer.ReadLine();
                        if (pong == null || !pong.Contains("PONG"))
                            throw new IOException();
                    }
                    catch
                    {
                        Console.WriteLine("[AGG] Heartbeat falhou — offline.");
                        serverConnected = false;
                        try { serverClient.Close(); } catch { }
                    }
                }
            }
        }
    }

    /// <summary>
    /// Envia o CSV para o servidor de armazenamento de forma assíncrona.
    /// </summary>
    /// <param name="wavyId">ID da WAVY</param>
    /// <param name="path">Caminho completo do ficheiro CSV local</param>
    /// <returns>True se o servidor respondeu CSV_RECEBIDO; caso contrário false.</returns>
    private static async Task<bool> EnviarCsvParaServidorFinalAsync(string wavyId, string path)
    {
        // 1) lê ficheiro de forma assíncrona
        byte[] conteudo = await File.ReadAllBytesAsync(path);
        string base64 = Convert.ToBase64String(conteudo);
        string fileName = Path.GetFileName(path);

        // 2) valida ligação
        lock (serverLock)
        {
            if (!serverConnected)
            {
                Console.WriteLine($"[AGG] Servidor offline – não envia CSV {fileName}");
                return false;
            }
        }

        try
        {
            // 3) envia comando de forma assíncrona
            await swServer.WriteLineAsync(
                $"ENVIAR_CSV;WAVY_ID={wavyId};FILENAME={fileName};DATA={base64}");

            // 4) aguarda resposta de forma assíncrona
            string? resp = await srServer.ReadLineAsync();

            if (resp != null && resp.Contains("CSV_RECEBIDO"))
            {
                Console.WriteLine($"[AGG] CSV enviado com sucesso: {fileName}");
                File.Delete(path);  // apaga local
                return true;
            }
            else
            {
                Console.WriteLine($"[AGG] Falha no envio CSV: {fileName}");
                return false;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[AGG] Erro TCP ao enviar CSV: {ex.Message}");
            serverConnected = false;
            return false;
        }
    }



    // ----------------------------------------------------
    private static async Task StartRabbitConsumerAsync()
    {
        var factory = new ConnectionFactory { HostName = "localhost" };
        await using var conn = await factory.CreateConnectionAsync();
        await using var ch = await conn.CreateChannelAsync();

        // exchange sensor.* -> fila efémera
        await ch.ExchangeDeclareAsync("wavy.readings", ExchangeType.Topic);
        var q = await ch.QueueDeclareAsync();
        await ch.QueueBindAsync(q.QueueName, "wavy.readings", "sensor.*");

        var consumer = new AsyncEventingBasicConsumer(ch);
        consumer.ReceivedAsync += async (_, ea) =>
        {
            var json = Encoding.UTF8.GetString(ea.Body.ToArray());

            string? wavyId = null;
            try
            {
                using var doc = JsonDocument.Parse(json);
                wavyId = doc.RootElement.GetProperty("wavyId").GetString();
            }
            catch { /* ignora JSON mal-formado */ }

            if (!string.IsNullOrWhiteSpace(wavyId))
            {
                Console.WriteLine($"[AGG] <-- {wavyId} : {json}");
                ProcessReading(wavyId, json);
            }

            await Task.CompletedTask;
        };

        await ch.BasicConsumeAsync(q.QueueName, true, consumer);
        Console.WriteLine("[AGG] Subscrito a RabbitMQ (sensor.*)");
        await Task.Delay(-1);           // evita sair
    }

    // ----------------------------------------------------
    private static void ProcessReading(string wavyId, string json)
    {
        // 1️⃣  Guarda no disco (por WAVY / dia)
        SaveLocal(wavyId, json, DateTime.UtcNow);

        bool flushNow = false;
        lock (batchLock)
        {
            if (!batchBuffers.ContainsKey(wavyId))
            {
                batchBuffers[wavyId] = new();
                lastFlush[wavyId] = DateTime.UtcNow;
            }

            batchBuffers[wavyId].Add(json);
            int count = batchBuffers[wavyId].Count;

            if (DateTime.UtcNow - lastFlush[wavyId] >= FLUSH_INTERVAL)
                flushNow = true;

            Console.WriteLine($"[AGG]    (buffer {wavyId}: {count} leituras)");
        }

        if (!flushNow) return;

        List<string> lote;
        lock (batchLock)
        {
            lote = new(batchBuffers[wavyId]);
            batchBuffers[wavyId].Clear();
            lastFlush[wavyId] = DateTime.UtcNow;
        }

        Console.WriteLine($"[AGG] **** FLUSH {wavyId} ({lote.Count} leituras) ****");
        _ = EnviarParaPreProcessamento(wavyId, lote);
    }

    private static void SaveLocal(string wavyId, string json, DateTime stamp)
    {
        string dir = Path.Combine("data", wavyId);        // data/WAVY001/
        Directory.CreateDirectory(dir);

        // ficheiro diário .json
        string file = Path.Combine(dir, stamp.ToString("yyyy-MM-dd") + ".json");
        File.AppendAllText(file, json + Environment.NewLine);
    }

    private static void ClearLocal(string wavyId, DateTime stamp)
    {
        string file = Path.Combine("data", wavyId, stamp.ToString("yyyy-MM-dd") + ".json");
        try { if (File.Exists(file)) File.Delete(file); } catch { /* ignora */ }
    }




    // ----------------------------------------------------
    private static async Task EnviarParaPreProcessamento(string wavyId, List<string> msgs)
    {
        var req = new LoteRequest { WavyId = wavyId };
        req.LeiturasJson.AddRange(msgs);

        Console.WriteLine($"[AGG] Enviar lote {msgs.Count} → PreProcess [{wavyId}]");
        try
        {
            var resp = await GrpcClient.ProcessarLoteAsync(req);
            if (!resp.Sucesso || resp.Csv.Length == 0)
            {
                Console.WriteLine("[AGG] (heartbeat ou falha) – nenhuma ação");
                return;
            }

            Console.WriteLine($"[AGG] RPC ok  ({resp.Csv.Length} bytes CSV)");

            // 1) grava os bytes directamente, sem converter para string
            Directory.CreateDirectory(Path.Combine("csv_out", wavyId));
            string path = Path.Combine("csv_out", wavyId,
                           $"{wavyId}_{DateTime.Now:yyyyMMdd_HHmm}.csv");
            var csvBytes = resp.Csv.ToByteArray();
            await File.WriteAllBytesAsync(path, csvBytes);
            Console.WriteLine($"[AGG] CSV gravado: {path}");

            // 2) envia AO SERVIDOR DE ARMAZENAMENTO uma única vez
            bool enviado = await EnviarCsvParaServidorFinalAsync(wavyId, path);
            Console.WriteLine(enviado
                ? $"[AGG] CSV enviado ao Servidor Final: {path}"
                : $"[AGG] Falha a enviar CSV: {path}");

            // 3) limpar locais só se correu tudo bem
            if (enviado)
            {
                // Apaga o ficheiro local e limpa o histórico diário
                File.Delete(path);
                ClearLocal(wavyId, DateTime.UtcNow);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[AGG] RPC erro : {ex.Message}");
        }
    }



}
