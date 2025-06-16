using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Pipes;
using System.Threading;

public class Master
{
    private static readonly Dictionary<string, Dictionary<string, int>> _aggregatedIndex = new Dictionary<string, Dictionary<string, int>>();
    private static readonly object _lock = new object();

    public static void Main(string[] args)
    {
        if (args.Length < 2)
        {
            Console.WriteLine("Usage: Master.exe <pipeName1> <pipeName2>");
            return;
        }

        try
        {
            Process.GetCurrentProcess().ProcessorAffinity = (IntPtr)0x0001;
            Console.WriteLine("Master process is running on CPU core 1.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Could not set processor affinity: {ex.Message}");
        }

        string pipeName1 = args[0];
        string pipeName2 = args[1];

        Thread agent1Thread = new Thread(ListenForAgent);
        Thread agent2Thread = new Thread(ListenForAgent);

        agent1Thread.Start(pipeName1);
        agent2Thread.Start(pipeName2);

        agent1Thread.Join();
        agent2Thread.Join();

        Console.WriteLine("\n--- Aggregated Word Index ---");
        foreach (var fileEntry in _aggregatedIndex)
        {
            foreach (var wordEntry in fileEntry.Value)
            {
                Console.WriteLine($"{fileEntry.Key}:{wordEntry.Key}:{wordEntry.Value}");
            }
        }
        Console.WriteLine("Master process finished.");
    }

    private static void ListenForAgent(object pipeNameObj)
    {
        string pipeName = (string)pipeNameObj;
        try
        {
            using (var server = new NamedPipeServerStream(pipeName))
            {
                Console.WriteLine($"Waiting for agent on pipe '{pipeName}' to connect...");
                server.WaitForConnection();
                Console.WriteLine($"Agent on pipe '{pipeName}' connected.");

                using (var reader = new StreamReader(server))
                {
                    string line;
                    while ((line = reader.ReadLine()) != null)
                    {
                        var parts = line.Split(':');
                        if (parts.Length == 3)
                        {
                            string fileName = parts[0];
                            string word = parts[1];
                            int count = int.Parse(parts[2]);

                            lock (_lock)
                            {
                                if (!_aggregatedIndex.ContainsKey(fileName))
                                {
                                    _aggregatedIndex[fileName] = new Dictionary<string, int>();
                                }
                                _aggregatedIndex[fileName][word] = count;
                            }
                        }
                    }
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error handling agent on pipe '{pipeName}': {ex.Message}");
        }
        finally
        {
            Console.WriteLine($"Agent on pipe '{pipeName}' has finished sending data.");
        }
    }
}