// WavyTempApp – envia leituras de temperatura via RabbitMQ
using RabbitMQ.Client;
using RabbitMQ.Client.Impl;
using System.Net;
using System.Text;
using System.Text.Json;


namespace WavyTempApp
{
    internal class Program
    {
        // ---------- configuração ----------
        private const string WAVY_ID = "WAVY003";
        private static readonly TimeSpan SEND_INTERVAL = TimeSpan.FromSeconds(15);
        private static readonly Random Rnd = new();

        // ---------- ponto de entrada ----------
        private static async Task Main(string[] args)
        {
            var factory = new ConnectionFactory { HostName = "localhost" };
            var cache = new List<string>(); //cache em memória;

            Console.WriteLine($"[WAVY] Simulador iniciado – ID {WAVY_ID}");
            Console.WriteLine("Prima Enter para terminar.\n");

            
            // loop de envio em tarefa separada
            _ = Task.Run(async () =>
            {
                IConnection? connection = null;
                IChannel? channel = null;

                while (true)
                {
                    await Task.Delay(SEND_INTERVAL);

                    // Simula leitura de temperatura
                    double wind = Math.Round(15 + Rnd.NextDouble() * 10, 2);
                    var payload = new
                    {
                        wavyId = WAVY_ID,
                        Wind = wind,
                        Timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")
                    };

                    string json = JsonSerializer.Serialize(payload);
                    cache.Add(json); // guarda na cache

                    try
                    {
                        // Reutiliza ligação se ainda estiver ativa
                        if (connection == null || !connection.IsOpen)
                            connection = await factory.CreateConnectionAsync();

                        if (channel == null || !channel.IsOpen)
                        {
                            channel = await connection.CreateChannelAsync();

                            await channel.ExchangeDeclareAsync(
                            exchange: "wavy.readings",
                            type: ExchangeType.Topic,
                            durable: false); // troca para true se já recriaste a exchange

                            Console.WriteLine($"[WAVY] Conectado a RabbitMQ – ID {WAVY_ID}");
                            Console.WriteLine("Prima Enter para terminar.\n");
                        }


                        var props = new BasicProperties
                        {
                            DeliveryMode = (DeliveryModes)2 // 2 = persistente
                        };

                        var address = new PublicationAddress(
                            ExchangeType.Topic, "wavy.readings", $"sensor.{WAVY_ID}");

                        foreach (var msg in cache)
                        {
                            byte[] body = Encoding.UTF8.GetBytes(msg);
                            await channel.BasicPublishAsync(address, props, body);
                            Console.WriteLine($"[WAVY] Enviado: {msg}");
                        }

                        cache.Clear(); // limpa a cache após envio
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[WAVY] Erro de envio – mensagem guardada. ({ex.Message})");

                        // se falhar, fecha a ligação e canal para tentar de novo no próximo loop
                        if (channel != null && channel.IsOpen)
                            await channel.CloseAsync();

                        if (connection != null && connection.IsOpen)
                            await connection.CloseAsync();

                        channel = null;
                        connection = null;
                    }
                }
            });


            Console.ReadLine();
        }
    }
}
