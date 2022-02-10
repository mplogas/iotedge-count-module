

namespace TotallyEmpty
{
    using Microsoft.Azure.Devices.Client;
    using Microsoft.Azure.Devices.Client.Transport.Mqtt;
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Text;
    using System.Text.Json;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Runtime.Loader;

    class Program
    {
        static int counter;
        private static double minConfidence = 0.5;

        static void Main(string[] args)
        {
#if DEBUG
            //string testJSON = "[{\"NEURAL_NETWORK\": [{\"bbox\": [0.365, 0.482, 0.902, 0.817], \"label\": \"Car\", \"confidence\": \"0.9253182\", \"timestamp\": \"1644434533400484665\"}]}]";
            string testJSON =
                "{\"NEURAL_NETWORK\": [{\"bbox\": [0.799, 0.740, 0.940, 0.904],\"label\": \"Car\", \"confidence\": \"0.932598\", \"timestamp\": \"1644415478571327732\"}, {\"bbox\": [0.575, 0.542, 0.608, 0.567],\"label\": \"Car\", \"confidence\": \"0.519588\", \"timestamp\": \"1644415478571327732\"}, {\"bbox\": [0.575, 0.542, 0.608, 0.567],\"label\": \"Truck\", \"confidence\": \"0.419588\", \"timestamp\": \"1644415478571327732\"}, {\"bbox\": [0.575, 0.542, 0.608, 0.567],\"label\": \"cat\", \"confidence\": \"0.519588\", \"timestamp\": \"1644415478571327732\"}]}";


            var c = ParseJson(testJSON);
            var j = BuildPayload(c);
#elif RELEASE
            Init().Wait();
            var cts = new CancellationTokenSource();
            AssemblyLoadContext.Default.Unloading += (ctx) => cts.Cancel();
            Console.CancelKeyPress += (sender, cpe) => cts.Cancel();
            WhenCancelled(cts.Token).Wait();
#endif            
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
            
            if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("TOTALLYEMPTY_MIN_CONFIDENCE")))
            {
                if(double.TryParse(Environment.GetEnvironmentVariable("TOTALLYEMPTY_MIN_CONFIDENCE"), out var conf)) minConfidence = conf;
            }
            Console.WriteLine($"min confidence: {minConfidence}");

            // Open a connection to the Edge runtime
            ModuleClient ioTHubModuleClient = await ModuleClient.CreateFromEnvironmentAsync(settings);
            await ioTHubModuleClient.OpenAsync();
            Console.WriteLine("IoT Hub module client initialized.");

            // Register callback to be called when a message is received by the module
            await ioTHubModuleClient.SetInputMessageHandlerAsync("input1", Count, ioTHubModuleClient);
        }

        /// <summary>
        /// This method is called whenever the module is sent a message from the EdgeHub. 
        /// It just pipe the messages without any change.
        /// It prints all the incoming messages.
        /// </summary>
        static async Task<MessageResponse> Count(Message message, object userContext)
        {
            int counterValue = Interlocked.Increment(ref counter);

            var moduleClient = userContext as ModuleClient;
            if (moduleClient == null)
            {
                throw new InvalidOperationException("UserContext doesn't contain " + "expected values");
            }

            byte[] messageBytes = message.GetBytes();
            string messageString = Encoding.UTF8.GetString(messageBytes);
            Console.WriteLine($"Received message: {counterValue}, Body: {messageString}");

            if (!string.IsNullOrEmpty(messageString))
            {
                try
                {
                    var countResult = ParseJson(messageString);
                    if (countResult.Count > 0)
                    {
                        var payload = BuildPayload(countResult);

                        var msg = new Message(Encoding.UTF8.GetBytes(payload));

                        await moduleClient.SendEventAsync("output1", msg);

                        Console.WriteLine("Received message sent");
                    }

                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                }


            }
            return MessageResponse.Completed;
        }

        private static Dictionary<string, int> ParseJson(string messageString)
        {
            var options = new JsonDocumentOptions
                {AllowTrailingCommas = true, CommentHandling = JsonCommentHandling.Skip};
            var result = new Dictionary<string, int>();

            using JsonDocument document = JsonDocument.Parse(messageString, options);
            foreach (var neuralNetwork in document.RootElement.EnumerateObject())
            {
                foreach (var element in neuralNetwork.Value.EnumerateArray())
                {
                    var label = element.GetProperty("label").GetString();

                    if (double.TryParse(element.GetProperty("confidence").GetString(), NumberStyles.AllowDecimalPoint, new NumberFormatInfo(), out double confidence))
                    {
                        if (confidence > minConfidence)
                        {
                            if (result.ContainsKey(label)) result[label]++;
                            else result[label] = 1;
                        }
                    }
                }
            }

            return result;
        }

        private static string BuildPayload(Dictionary<string, int> count)
        {
            var result = new
            {
                timestamp = DateTime.UtcNow,
                data = count.Select(i => new {label = i.Key, count = i.Value})
            };

            var json = JsonSerializer.Serialize(result);

            Console.WriteLine($"Json payload created {json}");
            return json;
        }
    }
}
