using System;
using BenchmarkDotNet.Running;

namespace DO71AlexeyChanges
{
  class Program
  {
    static void Main(string[] args)
    {
      BenchmarkRunner.Run<MSSQLQueriesBenchmarks>();
    }
  }
}
