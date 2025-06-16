using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.IO.Pipes;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;

public class Agent
{
    public static void Main(string[] args)
    {
        // Ensure the correct number of command-line arguments
        if (args.Length < 2)
        {
            Console.WriteLine("Usage: Agent.exe <directoryPath> <pipeName>");
            return;
        }

        string directoryPath = args[0];
        string pipeName = args[1];

        // Set the processor affinity
        try
        {
            if (pipeName.Contains("1"))
            {
                Process.GetCurrentProcess().ProcessorAffinity = (IntPtr)0x0002; // Assign to Core 2
                Console.WriteLine("Agent process is running on CPU core 2.");
            }
            else
            {
                Process.GetCurrentProcess().ProcessorAffinity = (IntPtr)0x0004; // Assign to Core 3
                Console.WriteLine("Agent process is running on CPU core 3.");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Could not set processor affinity: {ex.Message}");
        }


       
        var dataQueue = new BlockingCollection<string>();

        Thread indexerThread = new Thread(() => IndexFiles(directoryPath, dataQueue));
        Thread senderThread = new Thread(() => SendData(pipeName, dataQueue));

        indexerThread.Start();
        senderThread.Start();

        indexerThread.Join();
        senderThread.Join();

        Console.WriteLine("Agent process finished.");
    }

    
    private static void IndexFiles(string directoryPath, BlockingCollection<string> queue)
    {
        try
        {
            var txtFiles = Directory.GetFiles(directoryPath, "*.txt");
            foreach (var filePath in txtFiles)
            {
                var content = File.ReadAllText(filePath).ToLower();
                var words = Regex.Matches(content, @"\b\w+\b")
                                 .Cast<Match>()
                                 .Select(m => m.Value);

                var wordCount = words.GroupBy(w => w)
                                     .ToDictionary(g => g.Key, g => g.Count());

                foreach (var entry in wordCount)
                {
                    queue.Add($"{Path.GetFileName(filePath)}:{entry.Key}:{entry.Value}");
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error indexing files: {ex.Message}");
        }
        finally
        {
           
            queue.CompleteAdding();
        }
    }

   
    private static void SendData(string pipeName, BlockingCollection<string> queue)
    {
        try
        {
            using (var client = new NamedPipeClientStream(".", pipeName, PipeDirection.Out))
            {
                Console.WriteLine($"Connecting to pipe '{pipeName}'...");
                client.Connect(5000); // 5-second timeout
                Console.WriteLine("Connected.");

                using (var writer = new StreamWriter(client))
                {
                    foreach (var item in queue.GetConsumingEnumerable())
                    {
                        writer.WriteLine(item);
                        Console.WriteLine($"Sent: {item}");
                    }
                    writer.Flush(); 
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error sending data: {ex.Message}");
        }
    }
}