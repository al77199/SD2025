using Analysis;            // gerado por analysis.proto
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading;

namespace ServerApp
{
    // Representa o envelope de dados recebido do AGREGADOR 
    class Envelope
    {
        public string wavyId { get; set; }        // ID da WAVY de origem 
        public JsonElement[] batch { get; set; }  // Conjunto de dados (JSON) 
    }

    class Program
    {
        private static readonly object fileLock = new();  // Proteção de acesso aos ficheiros 


        // Função principal. Inicia o servidor na porta definida e aceita conexões dos 
        //AGREGADORES.


        static void Main(string[] args)
        {
            // 1) Arranca o listener TCP num thread em background
            new Thread(() => StartListener(6000))
            {
                IsBackground = true
            }.Start();

            // 2) Corre o menu (bloqueante) no thread principal
            RunConsoleMenuAsync().GetAwaiter().GetResult();
        }


        static void StartListener(int port)
        {
            Console.WriteLine($"[SERVIDOR] Iniciando na porta {port}...");
            var listener = new TcpListener(IPAddress.Any, port);
            listener.Start();

            while (true)
            {
                Console.WriteLine("[SERVIDOR] Aguardando conexões de Agregadores...");
                var aggClient = listener.AcceptTcpClient();
                Console.WriteLine("[SERVIDOR] Agregador conectado!");
                new Thread(() => HandleAggregator(aggClient))
                {
                    IsBackground = true
                }.Start();
            }
        }


        //-------------------------------MENU/ ENVIO PARA SERVIDOR---------------------------------------------
        /* static async Task RunConsoleMenuAsync()
         {
             using var channel = GrpcChannel.ForAddress("http://localhost:7001");
             var analysisClient = new AnalysisService.AnalysisServiceClient(channel);

             while (true)
             {
                 Console.WriteLine();
                 Console.WriteLine("=== Menu de Análise de Sensores ===");
                 Console.WriteLine("1) Temperatura");
                 Console.WriteLine("2) Precipitação (Rain)");
                 Console.WriteLine("3) Vento");
                 Console.WriteLine("Q) Sair");
                 Console.Write("Opção: ");
                 var key = Console.ReadLine()?.Trim().ToUpper();
                 if (key == "Q") break;

                 string column = key switch
                 {
                     "1" => "temperature_c",
                     "2" => "rain_mm",
                     "3" => "wind_kmh",
                     _ => null
                 };
                 if (column == null)
                 {
                     Console.WriteLine("Opção inválida.");
                     continue;
                 }

                 var since = DateTime.UtcNow.AddHours(-1);
                 //var baseDir = Path.Combine(AppContext.BaseDirectory, "..", "server_data");
                 var baseDir = Path.Combine(Directory.GetCurrentDirectory(), "server_data");

                 // AppContext.BaseDirectory => C:\...\ConsoleApp1\bin\Debug\net9.0\
                 /*var baseDir = Path.Combine(AppContext.BaseDirectory, "server_data");

                 if (!Directory.Exists(baseDir))
                 {
                     Console.WriteLine($"[ERRO] Não existe a pasta de dados: {baseDir}");
                     return;
                 }
                 *//*

                 foreach (var wavyDir in Directory.GetDirectories(baseDir))
                 {
                     string wavyId = Path.GetFileName(wavyDir);

                     // seleciona todos os CSV modificados na última hora
                     var files = Directory.GetFiles(wavyDir, "*.csv")
                         .Where(f => File.GetLastWriteTimeUtc(f) >= since)
                         .OrderBy(f => File.GetLastWriteTimeUtc(f));

                     foreach (var file in files)
                     {
                         byte[] csvBytes = await File.ReadAllBytesAsync(file);
                         var req = new AnalysisRequest
                         {
                             WavyId = wavyId,
                             Csv = ByteString.CopyFrom(csvBytes)
                         };

                         try
                         {
                             // usa AnalyzeAsync para não bloquear
                             var resp = await analysisClient.AnalyzeAsync(req);
                             Console.WriteLine($"[{wavyId}] avg={resp.Average:F2}, min={resp.Minimum:F2}, max={resp.Maximum:F2}");
                         }
                         catch (RpcException ex)
                         {
                             Console.WriteLine($"[ANALYSIS ERRO] {wavyId}: {ex.Status.Detail}");
                         }
                     }
                 }
             }
             Console.WriteLine("Saindo do menu de análise.");
         }*/
        static async Task RunConsoleMenuAsync()
        {
            // 1) Pontos de mapeamento
            var sensorMap = new Dictionary<string, (string wavyId, string sensorName)>()
            {
                ["1"] = ("WAVY001", "Temperatura"),
                ["2"] = ("WAVY002", "Precipitação"),
                ["3"] = ("WAVY003", "Vento")
            };

            // 2) Pasta base de onde saem os CSVs
            var baseDir = Path.Combine(Directory.GetCurrentDirectory(), "server_data");
            if (!Directory.Exists(baseDir))
            {
                Console.WriteLine($"[ERRO] Pasta de dados não encontrada: {baseDir}");
                return;
            }

            // 3) Cria o client gRPC
            using var channel = GrpcChannel.ForAddress("http://localhost:7001");
            var client = new AnalysisService.AnalysisServiceClient(channel);

            while (true)
            {
                // 4) Menu de sensores
                Console.WriteLine();
                Console.WriteLine("=== Menu de Análise de Sensores ===");
                Console.WriteLine("1) Temperatura  (WAVY001)");
                Console.WriteLine("2) Precipitação (WAVY002)");
                Console.WriteLine("3) Vento        (WAVY003)");
                Console.WriteLine("Q) Sair");
                Console.Write("Opção: ");

                var key = Console.ReadLine()?.Trim().ToUpper();
                if (key == "Q") break;

                if (!sensorMap.TryGetValue(key, out var info))
                {
                    Console.WriteLine("Opção inválida. Escolhe 1, 2, 3 ou Q.");
                    continue;
                }

                var (wavyId, sensorName) = info;
                Console.WriteLine($"\n[MENU] Enviando dados de {sensorName} ({wavyId})...\n");

                // 5) Prepara a pasta dessa WAVY
                string dir = Path.Combine(baseDir, wavyId);
                if (!Directory.Exists(dir))
                {
                    Console.WriteLine($"[ERRO] Diretório não existe: {dir}\n");
                    continue;
                }

                // 6) Filtra só os CSVs da última hora
                var since = DateTime.UtcNow.AddHours(-1);
                var files = Directory.GetFiles(dir, "*.csv")
                                     .Where(f => File.GetLastWriteTimeUtc(f) >= since)
                                     .OrderBy(f => File.GetLastWriteTimeUtc(f));

                if (!files.Any())
                {
                    Console.WriteLine($"[MENU] Sem ficheiros recentes em {wavyId}.\n");
                    continue;
                }

                // 7) Envia cada CSV por gRPC
                foreach (var file in files)
                {
                    byte[] csvBytes = await File.ReadAllBytesAsync(file);
                    var req = new AnalysisRequest
                    {
                        WavyId = wavyId,
                        Csv = ByteString.CopyFrom(csvBytes)
                    };

                    try
                    {
                        var resp = await client.AnalyzeAsync(req);
                        Console.WriteLine(
                            $"[{wavyId}] avg={resp.Average:F2}, " +
                            $"min={resp.Minimum:F2}, max={resp.Maximum:F2}");
                    }
                    catch (RpcException ex)
                    {
                        Console.WriteLine($"[ANALYSIS ERRO] {wavyId}: {ex.Status.Detail}");
                    }
                }

                Console.WriteLine();  // só para separar visualmente
            }

            Console.WriteLine("Saindo do menu de análise.");
        }


        // Gere a comunicação com um AGREGADOR. 
        // Ação: lê as mensagens recebidas, processa e responde. 

        static void HandleAggregator(TcpClient aggClient)
        {
            try
            {
                using var ns = aggClient.GetStream();
                using var sr = new StreamReader(ns);
                using var sw = new StreamWriter(ns) { AutoFlush = true };

                bool connected = false;
                string aggregatorId = "AGG-UNKNOWN";

                while (true)
                {
                    string line = sr.ReadLine();
                    if (line == null)
                    {
                        Console.WriteLine("[SERVIDOR] Conexão fechada pelo Agregador.");
                        break;
                    }
                    if (line.StartsWith("ENVIAR_CSV;"))
                    {
                        // Divide em 4 partes para extrair metadados
                        var parts = line.Split(';', 4);
                        var wavyPart = parts[1].Split('=', 2);
                        var filePart = parts[2].Split('=', 2);
                        string wavyId = wavyPart.Length > 1 ? wavyPart[1] : "(?)";
                        string fName = filePart.Length > 1 ? filePart[1] : "(?)";
                        int dataLen = parts[3].Length - "DATA=".Length;

                        Console.WriteLine($"[SERVIDOR] Recebido ENVIAR_CSV de {wavyId}, ficheiro {fName}, {dataLen} bytes");
                    }

                    else if (!line.Equals("PING", StringComparison.Ordinal))
                    {
                        Console.WriteLine("[SERVIDOR] Recebido => " + line);
                    }

                    string response = ProcessMessage(line, ref connected, ref aggregatorId);
                    sw.WriteLine(response);

                    if (!response.Equals("PONG", StringComparison.Ordinal))
                        Console.WriteLine("[SERVIDOR] Resposta => " + response);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("[SERVIDOR] Erro => " + ex.Message);
            }
            finally
            {
                aggClient.Close();
            }
        }

        /*static string ProcessMessage(string msg, ref bool connected, ref string aggregatorId)
        {
            //Console.WriteLine($"[SERVIDOR] Recebido => {msg}");

            // 1) Ping/heartbeat
            if (msg == "PING")
            {
                Console.WriteLine("[SERVIDOR] Respondendo PONG");
                return "PONG";
            }
            // 2) Handshake inicial
            else if (msg.StartsWith("PEDIDO_CONEXAO"))
            {
                connected = true;
                var parts = msg.Split(';');
                if (parts.Length > 1 && parts[1].StartsWith("AGG="))
                    aggregatorId = parts[1].Substring("AGG=".Length);
                Console.WriteLine($"[SERVIDOR] Agregador '{aggregatorId}' ligado");
                return "OK_CONEXAO";
            }
            // 3) Receção de CSV em base64
           else if (msg.StartsWith("ENVIAR_CSV") && connected)
            {
                msg = "ENVIAR_CSV;WAVY_ID=<id>;FILENAME=<fn>;DATA=<base64>"; // Simula o formato esperado
                var parts = msg.Split(';', 4);
                if (parts.Length < 4)
                {
                    Console.WriteLine("[SERVIDOR] ERRO_FORMATO_ENVIAR_CSV");
                    return "ERRO_FORMATO_ENVIAR_CSV";
                }

                string wavyId = parts[1].Substring("WAVY_ID=".Length);
                string fileName = parts[2].Substring("FILENAME=".Length);
                string base64 = parts[3].Substring("DATA=".Length);

                try
                {
                    byte[] csvBytes = Convert.FromBase64String(base64);
                    SaveCsvToFile(wavyId, fileName, csvBytes);
                    Console.WriteLine($"[SERVIDOR] CSV recebido de {wavyId}, gravado em server_data/{wavyId}/{fileName}");
                    return "CSV_RECEBIDO";
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[SERVIDOR] ERRO_PARSE_CSV: {ex.Message}");
                    return "ERRO_PARSE_CSV";
                }
            }
            // 4) Encerrar conexão
            else if (msg.StartsWith("END_CONN"))
            {
                Console.WriteLine("[SERVIDOR] Encerrando conexão a pedido");
                return "ACK_END_CONN";
            }
            // 5) Comando desconhecido ou não ligado ainda
            else
            {
                string resp = !connected ? "ERRO_NAO_CONECTADO" : "ERRO_COMANDO_DESCONHECIDO";
                Console.WriteLine($"[SERVIDOR] {resp}");
                return resp;
            }
        }*/

        static string ProcessMessage(string msg, ref bool connected, ref string aggregatorId)
        {
            // Só mostramos o msg “bruto” quando NÃO for CSV
            if (!msg.StartsWith("ENVIAR_CSV;"))
            {
                Console.WriteLine($"[SERVIDOR] Recebido => {msg}");
            }

            // 1) Heartbeat
            if (msg == "PING")
            {
                Console.WriteLine("[SERVIDOR] Respondendo PONG");
                return "PONG";
            }

            // 2) Handshake inicial
            else if (msg.StartsWith("PEDIDO_CONEXAO"))
            {
                connected = true;
                var parts = msg.Split(';');
                if (parts.Length > 1 && parts[1].StartsWith("AGG="))
                    aggregatorId = parts[1].Substring("AGG=".Length);
                Console.WriteLine($"[SERVIDOR] Agregador '{aggregatorId}' ligado");
                return "OK_CONEXAO";
            }

            // 3) Receção de CSV em base64
            else if (msg.StartsWith("ENVIAR_CSV") && connected)
            {
                try
                {
                    // encontra o índice onde começa ";DATA="
                    int idx = msg.IndexOf(";DATA=", StringComparison.Ordinal);
                    if (idx < 0)
                    {
                        Console.WriteLine("[SERVIDOR] ERRO_FORMATO_ENVIAR_CSV");
                        return "ERRO_FORMATO_ENVIAR_CSV";
                    }

                    // header contém "ENVIAR_CSV;WAVY_ID=...;FILENAME=..."
                    string header = msg.Substring(0, idx);
                    // dataPart é tudo depois de ";DATA="
                    string dataPart = msg.Substring(idx + ";DATA=".Length).Trim().Trim('"');

                    // extrai wavyId e fileName
                    var hparts = header.Split(';');
                    string wavyId = hparts[1].Split('=', 2)[1];
                    string fileName = hparts[2].Split('=', 2)[1];

                    // decodifica base64
                    byte[] csvBytes = Convert.FromBase64String(dataPart);

                    // grava no disco
                    SaveCsvToFile(wavyId, fileName, csvBytes);
                    Console.WriteLine($"[SERVIDOR] CSV recebido de {wavyId}, gravado em server_data/{wavyId}/{fileName}");
                    return "CSV_RECEBIDO";
                }
                catch (FormatException ex)
                {
                    Console.WriteLine($"[SERVIDOR] ERRO_PARSE_CSV: {ex.Message}");
                    return "ERRO_PARSE_CSV";
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[SERVIDOR] ERRO_INESPERADO_CSV: {ex.Message}");
                    return "ERRO_INESPERADO_CSV";
                }
            }

            // 4) Encerrar conexão
            else if (msg.StartsWith("END_CONN"))
            {
                Console.WriteLine("[SERVIDOR] Encerrando conexão a pedido");
                return "ACK_END_CONN";
            }

            // 5) Comando desconhecido / não ligado
            else
            {
                string resp = !connected ? "ERRO_NAO_CONECTADO" : "ERRO_COMANDO_DESCONHECIDO";
                Console.WriteLine($"[SERVIDOR] {resp}");
                return resp;
            }
        }




        // Grava os dados recebidos no sistema de ficheiros. 
        // Parâmetros: envelope – dados com ID da WAVY e lote de leituras. 
        // Ação: escreve cada linha com timestamp e dados JSON num ficheiro CSV. 
        static void SaveCsvToFile(string wavyId, string fileName, byte[] csvBytes)
        {
            string dir = Path.Combine("server_data", wavyId);
            Directory.CreateDirectory(dir);

            string path = Path.Combine(dir, fileName);
            lock (fileLock)
            {
                File.WriteAllBytes(path, csvBytes);
            }
        }

    }
}
