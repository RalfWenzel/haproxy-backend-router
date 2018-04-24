using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace gwrserver
{
    class Program
    {
        static void Main(string[] args)
        {
            // "listen-address": "127.0.0.1",
            // "listen-port": "7000",
            // "cache-init-file": "routing_table.csv",
            // "max-entries-backend": "100000"
            Dictionary<string, string> defaultConfig = new Dictionary<string, string>();
            defaultConfig.Add("listen-address", "127.0.0.1");
            defaultConfig.Add("listen-port", "7000");
            defaultConfig.Add("max-entries-backend", "100000");


            var builder = new ConfigurationBuilder()
                        .SetBasePath(Directory.GetCurrentDirectory())
                        .AddInMemoryCollection(defaultConfig)
                        .AddJsonFile("appsettings.json")
                        .AddCommandLine(args);
            IConfigurationRoot config = builder.Build();
            string listenAddress, cacheInitFile;
            int listenPort,maxEntriesBackend;
            listenAddress = config["listen-address"];
            listenPort = int.Parse( config["listen-port"] );
            maxEntriesBackend = int.Parse(config["max-entries-backend"]);
            TCPSocketServer socketServer = new TCPSocketServer(listenAddress, listenPort, maxEntriesBackend);
            if ( (cacheInitFile=config["cache-init-file"]) != null)
            {
                socketServer.InitCacheFromCSV(cacheInitFile, maxEntriesBackend);
            }
            socketServer.Run();
        }
    }

    public class TCPSocketServer
    {
        RoutingCache _cache = null;
        
        TcpListener _listener;
        // Testing only
        byte[] dcResponse = Encoding.ASCII.GetBytes("DC\n");
        byte[] genesisResponse = Encoding.ASCII.GetBytes("Genesis\n");
        byte[] invalidResponse = Encoding.ASCII.GetBytes("Invalid\n");
        public TCPSocketServer()
        {
            _listener = null;
        }
        public TCPSocketServer(string ipAddress, int port, int maxEntriesPerBackend) : this()
        {
            _cache = new RoutingCache(2*maxEntriesPerBackend);
            StartListener(ipAddress, port);
        }
        public void InitCacheFromCSV(string filename, int maxEntriesBackend )
        {
            LogInfo($"Load cache from {filename}.");
            _cache.InitFromCSV(filename, 1, ';', maxEntriesBackend);
            LogInfo($"Loaded total {_cache.Entries} entries. DC={_cache.DCEntries}, Genesis={_cache.GenesisEntries}.");
            if(_cache.Errors.Length > 0)
            {
                foreach( string errorStr in _cache.Errors )
                {
                    LogError($"Parser-{errorStr}");
                }
            }

        }
        public void StartListener(string ipAddress, int port)
        {
            IPAddress address = IPAddress.Parse(ipAddress);
            _listener = new TcpListener(address, port);
            _listener.Start(10240);
            LogInfo($"listener started on {ipAddress}:{port}");
        }

        private string GetLogMessage(string message)
        {
            DateTime dt = DateTime.UtcNow;
            return dt.ToString("yyyy-MM-dd hh:mm:ss") + " " + message;
        }

        public void LogError( string message)
        {
            Console.Error.WriteLine(GetLogMessage(message));
        }
        public void LogInfo(string message)
        {
            Console.WriteLine(GetLogMessage(message));
        }

        private byte[] GetBackend(string serial)
        {
            if (_cache == null) return invalidResponse;
            switch(  _cache.FromCache(serial))
            {
                case BackendCode.DC:
                    return dcResponse;
                case BackendCode.Genesis:
                    return genesisResponse;
                default:
                    return invalidResponse;
            }
        }
        public void Run()
        {
            bool quit = false;
            int acceptRetries = 0;
            while ( !quit )
            {
                var clientTask = _listener.AcceptTcpClientAsync();
                if(clientTask==null)
                {
                    LogError($"TcpListener.AcceptTcpClientAsync() returned null. Retries={++acceptRetries}.");
                    if ( acceptRetries == 10)
                    {
                        quit = true;
                        continue;
                    }
                    Thread.Sleep(1000);
                    continue;
                }
                else
                {
                    acceptRetries = 0;
                    var client = clientTask.Result;
                    string message = "";
                    byte[] buffer = new byte[128];
                    while ( true )
                    {
                        try
                        {
                            int intresult = client.GetStream().Read(buffer, 0, buffer.Length);
                            if (intresult == 0) break;
                            message = Encoding.ASCII.GetString(buffer,0,intresult).Replace("\n","").Replace("\r","");

                            if (String.IsNullOrEmpty(message))
                            {
                                client.GetStream().Write(invalidResponse, 0, invalidResponse.Length); 
                                break;
                            }
                            if (message[0] == 's')
                            {
                                string serial = message.Substring(1);
                                byte[] beMessage = GetBackend(serial);
                                client.GetStream().Write(beMessage, 0, beMessage.Length);
                            }
                            else if (message[0] == 'q')
                            {
                                quit = true;
                                break;
                            }
                            else
                            {
                                LogError($"Unexpected message {message}.");
                                client.GetStream().Write(invalidResponse, 0, invalidResponse.Length);
                                break;
                            }
                        }
                        catch( Exception ex)
                        {
                            LogError($"Read exception: {ex.ToString()}");
                            Thread.Sleep(200);
                            break;
                        }
                    }
                    try
                    {
                        client.GetStream().Dispose();
                    }
                    catch (Exception ex) { }
                }
            }
        }
    }
    
}
