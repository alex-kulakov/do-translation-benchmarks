using System;
using BenchmarkDotNet.Running;

namespace DO71SergeyChanges
{
  class Program
  {
    static void Main(string[] args)
    {
      BenchmarkRunner.Run<MSSQLQueriesBenchmarks>();
    }
  }
}
