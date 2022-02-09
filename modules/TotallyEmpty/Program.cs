using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace TotallyEmpty
{
    using System;
    using System.IO;
    using System.Runtime.InteropServices;
    using System.Runtime.Loader;
    using System.Security.Cryptography.X509Certificates;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Devices.Client;
    using Microsoft.Azure.Devices.Client.Transport.Mqtt;

    class Program
    {
        static int counter;
        private static string label = "Car";
        private static double minConfidence = 0.5;

        private const string testJSON =
            @"[{'NEURAL_NETWORK': [{'bbox': [0.799, 0.740, 0.940, 0.904],'label': 'Car', 'confidence': '0.932598', 'timestamp': '1644415478571327732'}, {'bbox': [0.575, 0.542, 0.608, 0.567],'label': 'Car', 'confidence': '0.519588', 'timestamp': '1644415478571327732'}]}]";

        static void Main(string[] args)
        {
            Init().Wait();

            // Wait until the app unloads or is cancelled
            var cts = new CancellationTokenSource();
            AssemblyLoadContext.Default.Unloading += (ctx) => cts.Cancel();
            Console.CancelKeyPress += (sender, cpe) => cts.Cancel();
            WhenCancelled(cts.Token).Wait();
        }


        /// <summary>
        /// Handles cleanup operations when app is cancelled or unloads
        /// </summary>
        public static Task WhenCancelled(CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>();
            cancellationToken.Register(s => ((TaskCompletionSource<bool>)s).SetResult(true), tcs);
            return tcs.Task;
        }

        /// <summary>
        /// Initializes the ModuleClient and sets up the callback to receive
        /// messages containing temperature information
        /// </summary>
        static async Task Init()
        {
            MqttTransportSettings mqttSetting = new MqttTransportSettings(TransportType.Mqtt_Tcp_Only);
            ITransportSettings[] settings = { mqttSetting };

            if(!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("TOTALLYEMPTY_LABEL"))) label = Environment.GetEnvironmentVariable("TOTALLYEMPTY_LABEL");
            if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("TOTALLYEMPTY_MIN_CONFIDENCE")))
            {
                if(double.TryParse(Environment.GetEnvironmentVariable("TOTALLYEMPTY_MIN_CONFIDENCE"), out var conf)) minConfidence = conf;
            }
            Console.WriteLine($"Active label: {label}, min confidence: {minConfidence}");

            // Open a connection to the Edge runtime
            ModuleClient ioTHubModuleClient = await ModuleClient.CreateFromEnvironmentAsync(settings);
            await ioTHubModuleClient.OpenAsync();
            Console.WriteLine("IoT Hub module client initialized.");

            // Register callback to be called when a message is received by the module
            await ioTHubModuleClient.SetInputMessageHandlerAsync("input1", CountCars, ioTHubModuleClient);
        }

        /// <summary>
        /// This method is called whenever the module is sent a message from the EdgeHub. 
        /// It just pipe the messages without any change.
        /// It prints all the incoming messages.
        /// </summary>
        static async Task<MessageResponse> CountCars(Message message, object userContext)
        {
            int counterValue = Interlocked.Increment(ref counter);

            var moduleClient = userContext as ModuleClient;
            if (moduleClient == null)
            {
                throw new InvalidOperationException("UserContext doesn't contain " + "expected values");
            }

            byte[] messageBytes = message.GetBytes();
            string messageString = Encoding.UTF8.GetString(messageBytes);
            Console.WriteLine($"Received message: {counterValue}, Body: [{messageString}]");

            if (!string.IsNullOrEmpty(messageString))
            {
                var count = ParseJson(messageString, label);
                var payload = BuildPayload(count, label);

                var msg = new Message(Encoding.UTF8.GetBytes(payload));

                await moduleClient.SendEventAsync("output1", msg);

                Console.WriteLine("Received message sent");
            }
            return MessageResponse.Completed;
        }

        private static int ParseJson(string messageString, string tag)
        {
            dynamic nnArrays = JArray.Parse(messageString);
            dynamic neuralNetwork = nnArrays[0];
            dynamic data = JArray.Parse(neuralNetwork.NEURAL_NETWORK.ToString());
            var result = 0;
            foreach (dynamic detect in data)
            {
                if (detect.label == tag) result++;
            }

            Console.WriteLine($"{label}s detected: {result}");
            return result;
        }

        private static string BuildPayload(int count, string tag)
        {
            var sb = new StringBuilder();
            sb.Append("{");
            sb.Append("\"label\":\"");
            sb.Append(tag);
            sb.Append("\",");
            sb.Append("\"amount\":\"");
            sb.Append(count);
            sb.Append("\",");
            sb.Append("\"timestamp\":\"");
            sb.Append(DateTime.UtcNow);
            sb.Append("\"");
            sb.Append("}");

            Console.WriteLine($"Json payload created {sb.ToString()}");
            return sb.ToString();
        }
    }
}
