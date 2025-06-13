using System;
using System.Threading.Tasks;
using Grpc.Core;
using Analysis;

namespace AnalysisCoreServer
{
    class Program
    {
        const int PORT = 7001;

        static async Task Main(string[] args)
        {
            // 1) Cria e configura o servidor
            var server = new Server
            {
                Services = { AnalysisService.BindService(new AnalysisServiceImpl()) },
                Ports = { new ServerPort("localhost", PORT, ServerCredentials.Insecure) }
            };

            // 2) Arranca
            server.Start();
            Console.WriteLine($"[ANALYSIS] Servidor gRPC a escutar em localhost:{PORT}");
            Console.WriteLine("Prima ENTER para terminar…");
            Console.ReadLine();

            // 3) Fecha graciosamente
            await server.ShutdownAsync();
        }
    }
}
