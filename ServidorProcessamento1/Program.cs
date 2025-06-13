using Grpc.Core;
using PreProcess;  // gerado a partir do .proto

const int PORT = 5100;

var server = new Server
{
    Ports = { new ServerPort("localhost", PORT, ServerCredentials.Insecure) },
    Services = { PreProcessService.BindService(new PreProcessServiceImpl()) }
};

server.Start();
Console.WriteLine($"[PROC] Servidor gRPC a escutar em localhost:{PORT}");
Console.WriteLine("Prima ENTER para terminar…");
Console.ReadLine();

await server.ShutdownAsync();
