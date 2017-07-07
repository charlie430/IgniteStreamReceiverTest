using System;
using Apache.Ignite.Core;

namespace Server {
    internal class Program {
        private static void Main(string[] args) {
            Ignition.Start();
            Console.WriteLine("Press [enter] to exit...");
            Console.Read();
        }
    }
}