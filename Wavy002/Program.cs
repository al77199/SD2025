// WavyTempApp – envia leituras de temperatura via RabbitMQ
using RabbitMQ.Client;
using System.Text;
using System.Text.Json;

namespace WavyTempApp
{
    internal class Program
    {
        // ---------- configuração ----------
        private const string WAVY_ID = "WAVY002";
        private static readonly TimeSpan SEND_INTERVAL = TimeSpan.FromSeconds(15);
        private static readonly Random Rnd = new();

        // ---------- ponto de entrada ----------
        private static async Task Main(string[] args)
        {
            var factory = new ConnectionFactory { HostName = "localhost" };

            await using var connection = await factory.CreateConnectionAsync();
            await using var channel = await connection.CreateChannelAsync();

            // Exchange do tipo Topic: permite routing-keys flexíveis
            await channel.ExchangeDeclareAsync("wavy.readings", ExchangeType.Topic);

            Console.WriteLine($"[WAVY] Conectado a RabbitMQ – ID {WAVY_ID}");
            Console.WriteLine("Prima Enter para terminar.\n");

            // loop de envio em tarefa separada
            _ = Task.Run(async () =>
            {
                while (true)
                {
                    await Task.Delay(SEND_INTERVAL);

                    double rain = Math.Round(Rnd.NextDouble() * 100, 1);
                    var payload = new
                    {
                        wavyId = WAVY_ID,
                        Rain = rain,
                        timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")
                    };

                    string json = JsonSerializer.Serialize(payload);
                    byte[] body = Encoding.UTF8.GetBytes(json);
                    
                    // propriedades (content-type JSON opcional)
                    var props = new BasicProperties
                    {
                        ContentType = "application/json",
                        DeliveryMode = (DeliveryModes)2          // 1=transient, 2=persistent
                    };

                    // routing-key: sensor.<ID> (ex.: sensor.WAVY002)
                    await channel.BasicPublishAsync(
                        exchange: "wavy.readings",
                        routingKey: $"sensor.{WAVY_ID}", 
                        mandatory :false,
                        basicProperties: props,
                        body: body);

                    Console.WriteLine($"[WAVY] => {json}");
                }
            });

            Console.ReadLine();
        }
    }
}
