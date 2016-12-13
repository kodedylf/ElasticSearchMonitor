using System;
using System.IO.Ports;
using System.Linq;
using System.Text;
using System.Threading;
using Nest;

namespace ElasticSearchMonitor
{
    class Program
    {
        public class Fields
        {
            public string method { get; set; }
            public string traceId { get; set; }
            public string source { get; set; }
        }
        [ElasticsearchType(Name = "logevent")]
        public class Logevent
        {
            [Date(Name = "@timestamp")]
            public DateTime timestamp { get; set; }
            public string level { get; set; }
            public Fields fields { get; set; }
        }
        static void Main(string[] args)
        {
            if (args[0] == "test")
            {
                var serialPort = new SerialPort("COM3", 9600, Parity.None, 8, StopBits.One);
                serialPort.Open();
                serialPort.Write(Encoding.ASCII.GetBytes("A"), 0, 1);
                serialPort.Close();
            }
            else
            {
                DateTime latestPurchaseApproval = new DateTime(1000, 1, 1);
                // var settings = new ConnectionSettings(new Uri(@"http://wx0855:9200")).DefaultIndex("engine-syst");
                var settings = new ConnectionSettings(new Uri(@"http://y01089:9200")).DefaultIndex("engine-prod");
                var client = new ElasticClient(settings);
                var serialPort = new SerialPort("COM3", 9600, Parity.None, 8, StopBits.One);
                serialPort.Open();

                while (true)
                {
                    var response = client.Search<Logevent>(s => s
                                            .AllTypes()
                                            .Sort(ss => ss.Descending(p => p.timestamp))
                                            .Size(1)
                                            .Query(q => q.Term(p => p.fields.method, "Decision.Flow.ApplicationService.MakeDecisionApplicationService.MakeDecision")));
                    var entry = response.Documents.First();
                    if (latestPurchaseApproval.Year == 1000)
                    {
                        latestPurchaseApproval = entry.timestamp;
                    }
                    else
                    {
                        if (entry.timestamp > latestPurchaseApproval)
                        {
                            Console.WriteLine("Yay! New purchase approval at {0}",
                                entry.timestamp.ToString("yyyy-MM-dd HH.mm.ss"));
                            latestPurchaseApproval = entry.timestamp;
                            serialPort.Write(Encoding.ASCII.GetBytes("A"), 0, 1);
                        }
                    }
                    Thread.Sleep(10000);
                }
            }
        }
    }
}
