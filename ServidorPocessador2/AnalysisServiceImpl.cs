using System;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Analysis;
using Grpc.Core;

namespace AnalysisCoreServer
{
    public class AnalysisServiceImpl : AnalysisService.AnalysisServiceBase
    {
        public override Task<AnalysisResponse> Analyze(
            AnalysisRequest request, ServerCallContext context)
        {
            // 1) Decodifica CSV recebido (UTF-8)
            string csvText = Encoding.UTF8.GetString(request.Csv.ToByteArray());

            // 2) Quebra em linhas (ignora linhas vazias)
            var lines = csvText
                .Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);

            if (lines.Length < 2)
            {
                // Sem dados suficientes
                return Task.FromResult(new AnalysisResponse
                {
                    WavyId = request.WavyId,
                    Average = 0,
                    Minimum = 0,
                    Maximum = 0
                });
            }

            // 3) Percorre as linhas, extrai a segunda coluna (valor)
            double sum = 0, mn = double.MaxValue, mx = double.MinValue;
            int count = 0;
            for (int i = 1; i < lines.Length; i++)
            {
                var cols = lines[i].Split(';');
                if (cols.Length < 2) continue;

                // Substitui vírgula decimal por ponto
                string cell = cols[1].Replace(',', '.');

                if (double.TryParse(
                    cell,
                    NumberStyles.Any,
                    CultureInfo.InvariantCulture,
                    out double v))
                {
                    sum += v;
                    mn = Math.Min(mn, v);
                    mx = Math.Max(mx, v);
                    count++;
                }
            }

            double avg = count > 0 ? sum / count : 0;

            Console.WriteLine(
              $"[ANALYSIS] {request.WavyId}: avg={avg:F2}, min={mn:F2}, max={mx:F2}");

            // 4) Devolve resposta
            return Task.FromResult(new AnalysisResponse
            {
                WavyId = request.WavyId,
                Average = avg,
                Minimum = count > 0 ? mn : 0,
                Maximum = count > 0 ? mx : 0
            });
        }
    }
}
