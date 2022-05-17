using System;
using BenchmarkDotNet.Running;

namespace DO70CurrentBenchmarks
{
  class Program
  {
    static void Main(string[] args)
    {
      BenchmarkRunner.Run<MSSQLQueriesBenchmarks>();
    }
  }
}
