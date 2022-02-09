using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Transactions;
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
        private static double minConfidence = 0.5;

        private const string testJSON =
            @"[{'NEURAL_NETWORK': [{'bbox': [0.799, 0.740, 0.940, 0.904],'label': 'Car', 'confidence': '0.932598', 'timestamp': '1644415478571327732'}, {'bbox': [0.575, 0.542, 0.608, 0.567],'label': 'Car', 'confidence': '0.519588', 'timestamp': '1644415478571327732'}, {'bbox': [0.575, 0.542, 0.608, 0.567],'label': 'Truck', 'confidence': '0.419588', 'timestamp': '1644415478571327732'}, {'bbox': [0.575, 0.542, 0.608, 0.567],'label': 'cat', 'confidence': '0.519588', 'timestamp': '1644415478571327732'}]}]";

        static void Main(string[] args)
        {
            //var c = ParseJson(testJSON);
            //var j = BuildPayload(c);

            Init().Wait();
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
                try
                {
                    var count = ParseJson(messageString);
                    var payload = BuildPayload(count); 
                    
                    var msg = new Message(Encoding.UTF8.GetBytes(payload));

                    await moduleClient.SendEventAsync("output1", msg);

                    Console.WriteLine("Received message sent");
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
            dynamic nnArrays = JArray.Parse(messageString);
            dynamic neuralNetwork = nnArrays[0];
            dynamic data = JArray.Parse(neuralNetwork.NEURAL_NETWORK.ToString());

            var result = new Dictionary<string, int>();

            foreach (dynamic detect in data)
            {
                if (double.TryParse(detect.confidence.ToString(), NumberStyles.AllowDecimalPoint, new NumberFormatInfo(), out double confidence))
                {
                    if (confidence > minConfidence)
                    {
                        if (result.ContainsKey(detect.label.ToString())) result[detect.label.ToString()]++;
                        else result[detect.label.ToString()] = 1;
                    }
                }
            }

            return result;
        }

        private static string BuildPayload(Dictionary<string, int> count)
        {
            var result = new
            {
                timestamp = DateTime.UtcNow.Ticks,
                data = count.Select(i => new {label = i.Key, count = i.Value})
            };

            var json = JsonConvert.SerializeObject(result);

            Console.WriteLine($"Json payload created {json}");
            return json;
        }
    }
}
