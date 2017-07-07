using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Binary;
using Apache.Ignite.Core.Cache.Configuration;
using Common;

namespace Client {
    internal class Program {
        private const string CacheName = "myCache";
        private const string TableName = "myTable";
        private const string PrimaryKeyName = "rowId";
        private static readonly Type PrimaryKeyType = typeof(string);
        private const string RowNumberName = "rowNumber";
        private static readonly Type RowNumberType = typeof(int);

        private static void Main(string[] args) {
            Ignition.ClientMode = true;

            var ignite = Ignition.Start();

            var fields = new List<QueryField>(2) {
                new QueryField(PrimaryKeyName, PrimaryKeyType) {
                    IsKeyField = true
                },
                new QueryField(RowNumberName, RowNumberType)
            };
            var indexes = new List<QueryIndex>(2) {
                new QueryIndex(false, PrimaryKeyName),
                new QueryIndex(false, QueryIndexType.Sorted, RowNumberName)
            };

            var cacheConfig = new CacheConfiguration {
                Name = CacheName,
                CacheMode = CacheMode.Partitioned,

                QueryEntities = new[] {
                    new QueryEntity {
                        KeyType = PrimaryKeyType,
                        ValueTypeName = TableName,
                        Fields = fields,
                        Indexes = indexes
                    }
                }
            };

            ignite.GetOrCreateCache<string, object>(cacheConfig)
                .WithKeepBinary<string, IBinaryObject>();

            using (var ldr = ignite.GetDataStreamer<string, IBinaryObject>(CacheName)) {
                ldr.AllowOverwrite = true;
                ldr.Receiver = new RowStreamReceiver();

                Parallel.ForEach(Enumerable.Range(0, 100), i => {
                    var pair = BuildRow(ignite, i);
                    ldr.AddData(pair);
                });

                ldr.Flush();
            }

            Console.WriteLine("Press [enter] to exit...");
            Console.Read();
        }

        private static KeyValuePair<string, IBinaryObject> BuildRow(IIgnite ignite, int i) {
            var builder = ignite.GetBinary().GetBuilder(CacheName);

            // Add standard columns
            var rowId = Guid.NewGuid().ToString("N");
            builder.SetField(PrimaryKeyName, rowId);
            builder.SetField(RowNumberName, i);

            return new KeyValuePair<string, IBinaryObject>(rowId, builder.Build());
        }
    }
}