using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Ignite.Core.Binary;
using Apache.Ignite.Core.Cache;
using Apache.Ignite.Core.Datastream;

namespace Common {
    [Serializable]
    public class RowStreamReceiver : IStreamReceiver<string, IBinaryObject> {
        public void Receive(ICache<string, IBinaryObject> cache, ICollection<ICacheEntry<string, IBinaryObject>> entries) {
            var binary = cache.Ignite.GetBinary();

            cache.PutAll(entries.ToDictionary(x => x.Key, x => {
                var builder = binary.GetBuilder(x.Value);
                SetColumnFields(builder);
                return builder.Build();
            }));
        }

        // Add dynamic columns
        private static void SetColumnFields(IBinaryObjectBuilder builder) {
            Enumerable.Range(0, 3)
                .ToList()
                .ForEach(i => builder.SetIntField($"c{i}", i));
        }
    }
}