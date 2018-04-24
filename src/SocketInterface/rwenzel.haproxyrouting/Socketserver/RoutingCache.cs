using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace rwenzel.haproxyrouting.socketserver
{
    public enum BackendCode
    {
        NotFound = 0,
        DC = 1,
        Genesis = 2,
        Invalid = 3
    }

    public class RoutingCache
    {
        Dictionary<string, BackendCode> _beMapping;
        int _dcEntries = 0;
        int _genesisEntries = 0;
        List<string> _errors = new List<string>();

        public int DCEntries {  get { return _dcEntries;  } }
        public int GenesisEntries { get { return _genesisEntries; } }
        public int Entries { get { return _beMapping.Count; } }
        public string[] Errors {  get { return _errors.ToArray(); } }

        public RoutingCache() : this( 100000 )
        {

        }
        public RoutingCache(int cacheSize) 
        {
            _beMapping = new Dictionary<string, BackendCode>(cacheSize);
        }
        public BackendCode FromCache(string serial)
        {
            if(_beMapping.ContainsKey(serial))
            {
                return _beMapping[serial];
            }
            return BackendCode.NotFound;
        }

        void AddError(string error)
        {
            if( _errors.Count == 10)
            {
                throw new Exception("Too many errors!");
            }
            _errors.Add(error);

        }
        public void InitFromCSV(string fileName, int skip, char separator, int maxEntriesBackend)
        {
            string[] mapping = File.ReadAllLines(fileName, Encoding.UTF8);
            for( int line = skip; line < mapping.Length; line++)
            {
                string[] token = mapping[line].Split(separator);
                if (token.Length != 2)
                {
                    AddError($"Line {line}: found {token.Length} token(s) in '{mapping[line]}'.");
                    continue;
                }
                if (token[0].Length != 16)
                {
                    AddError($"Line {line}: Serial length={token[0].Length} in '{mapping[line]}'.");
                    continue;
                }
                if ( token[1] == "Genesis")
                {
                    if (_genesisEntries > maxEntriesBackend) continue;
                    if( _genesisEntries == maxEntriesBackend )
                    {
                        AddError($"Line {line}: {maxEntriesBackend} mappings for 'Genesis' backends reached. Ignoring subsequent entries.");
                        _genesisEntries++;
                        continue;
                    }
                    _beMapping.Add(token[0], BackendCode.Genesis);
                    _genesisEntries++;
                    continue;
                }
                if(token[1] == "DC")
                {
                    if (_dcEntries > maxEntriesBackend) continue;
                    if (_dcEntries == maxEntriesBackend)
                    {
                        AddError($"Line {line}: {maxEntriesBackend} mappings for 'DC' backends reached. Ignoring subsequent entries.");
                        _dcEntries++;
                        continue;
                    }
                    _beMapping.Add(token[0], BackendCode.DC);
                    _dcEntries++;
                    continue;
                }
                AddError( $"Line {line}: Unknown backend {token[1]} in '{mapping[line]}'.");
            } // next line
        }
    }
}
