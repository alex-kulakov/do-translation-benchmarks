using BenchmarkDotNet.Attributes;

namespace DO70CurrentBenchmarks
{
  [MemoryDiagnoser]
  [Orderer(BenchmarkDotNet.Order.SummaryOrderPolicy.Declared)]
  [RankColumn]
  public class MSSQLQueriesBenchmarks
  {
    private readonly MSSQLQueries theTests;

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void ParameterInWhereCase()
    {
      var q = theTests.ParameterInWhere();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void ALotOfColumns()
    {
      var q = theTests.ALotOfColumns();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void ALotOfColumnsWithQueryAsSource()
    {
      var q = theTests.ALotOfColumnsWithQueryAsSource();
    }

    #region Selects

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select001()
    {
      var q = theTests.Select001();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select002()
    {
      var q = theTests.Select002();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select003()
    {
      var q = theTests.Select003();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select004()
    {
      var q = theTests.Select004();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select005()
    {
      var q = theTests.Select005();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select006()
    {
      var q = theTests.Select006();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select007()
    {
      var q = theTests.Select007();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select008()
    {
      var q = theTests.Select008();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select009()
    {
      var q = theTests.Select009();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select010()
    {
      var q = theTests.Select010();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select011()
    {
      var q = theTests.Select011();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select012()
    {
      var q = theTests.Select012();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select013()
    {
      var q = theTests.Select013();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select014()
    {
      var q = theTests.Select014();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select015()
    {
      var q = theTests.Select015();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select016()
    {
      var q = theTests.Select016();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select017()
    {
      var q = theTests.Select017();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select018()
    {
      var q = theTests.Select018();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select019()
    {
      var q = theTests.Select019();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select020()
    {
      var q = theTests.Select020();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select021()
    {
      var q = theTests.Select021();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select022()
    {
      var q = theTests.Select022();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select023()
    {
      var q = theTests.Select023();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select024()
    {
      var q = theTests.Select024();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select025()
    {
      var q = theTests.Select025();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select026()
    {
      var q = theTests.Select026();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select027()
    {
      var q = theTests.Select027();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select028()
    {
      var q = theTests.Select028();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select029()
    {
      var q = theTests.Select029();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select030()
    {
      var q = theTests.Select030();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select031()
    {
      var q = theTests.Select031();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select032()
    {
      var q = theTests.Select032();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select033()
    {
      var q = theTests.Select033();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select034()
    {
      var q = theTests.Select034();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select035()
    {
      var q = theTests.Select035();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select036()
    {
      var q = theTests.Select036();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select037()
    {
      var q = theTests.Select037();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select038()
    {
      var q = theTests.Select038();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select039()
    {
      var q = theTests.Select039();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select040()
    {
      var q = theTests.Select040();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select041()
    {
      var q = theTests.Select041();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select042()
    {
      var q = theTests.Select042();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select043()
    {
      var q = theTests.Select043();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select044()
    {
      var q = theTests.Select044();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select045()
    {
      var q = theTests.Select045();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select046()
    {
      var q = theTests.Select046();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select047()
    {
      var q = theTests.Select047();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select048()
    {
      var q = theTests.Select048();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select049()
    {
      var q = theTests.Select049();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select050()
    {
      var q = theTests.Select050();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select051()
    {
      var q = theTests.Select051();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select052()
    {
      var q = theTests.Select052();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select053()
    {
      var q = theTests.Select053();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select054()
    {
      var q = theTests.Select054();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select055()
    {
      var q = theTests.Select055();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select056()
    {
      var q = theTests.Select056();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select057()
    {
      var q = theTests.Select057();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select058()
    {
      var q = theTests.Select058();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select059()
    {
      var q = theTests.Select059();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select060()
    {
      var q = theTests.Select060();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select061()
    {
      var q = theTests.Select061();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select062()
    {
      var q = theTests.Select062();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select063()
    {
      var q = theTests.Select063();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select064()
    {
      var q = theTests.Select064();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select065()
    {
      var q = theTests.Select065();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select066()
    {
      var q = theTests.Select066();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select067()
    {
      var q = theTests.Select067();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select068()
    {
      var q = theTests.Select068();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select069()
    {
      var q = theTests.Select069();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select070()
    {
      var q = theTests.Select070();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select071()
    {
      var q = theTests.Select071();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select072()
    {
      var q = theTests.Select072();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select073()
    {
      var q = theTests.Select073();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select074()
    {
      var q = theTests.Select074();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select075()
    {
      var q = theTests.Select075();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select076()
    {
      var q = theTests.Select076();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select077()
    {
      var q = theTests.Select077();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select078()
    {
      var q = theTests.Select078();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select079()
    {
      var q = theTests.Select079();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select080()
    {
      var q = theTests.Select080();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select081()
    {
      var q = theTests.Select081();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select082()
    {
      var q = theTests.Select082();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select083()
    {
      var q = theTests.Select083();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select084()
    {
      var q = theTests.Select084();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select085()
    {
      var q = theTests.Select085();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select086()
    {
      var q = theTests.Select086();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select087()
    {
      var q = theTests.Select087();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select088()
    {
      var q = theTests.Select088();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select089()
    {
      var q = theTests.Select089();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select090()
    {
      var q = theTests.Select090();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select091()
    {
      var q = theTests.Select091();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select092()
    {
      var q = theTests.Select092();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select093()
    {
      var q = theTests.Select093();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select094()
    {
      var q = theTests.Select094();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select095()
    {
      var q = theTests.Select095();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select096()
    {
      var q = theTests.Select096();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select097()
    {
      var q = theTests.Select097();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select098()
    {
      var q = theTests.Select098();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select099()
    {
      var q = theTests.Select099();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select100()
    {
      var q = theTests.Select100();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select101()
    {
      var q = theTests.Select101();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select102()
    {
      var q = theTests.Select102();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select103()
    {
      var q = theTests.Select103();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select104()
    {
      var q = theTests.Select106();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select105()
    {
      var q = theTests.Select105();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select106()
    {
      var q = theTests.Select106();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select107()
    {
      var q = theTests.Select107();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select108()
    {
      var q = theTests.Select108();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select109()
    {
      var q = theTests.Select109();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select110()
    {
      var q = theTests.Select110();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select111()
    {
      var q = theTests.Select111();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select112()
    {
      var q = theTests.Select112();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select113()
    {
      var q = theTests.Select113();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select114()
    {
      var q = theTests.Select114();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select115()
    {
      var q = theTests.Select115();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select116()
    {
      var q = theTests.Select116();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select117()
    {
      var q = theTests.Select117();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select118()
    {
      var q = theTests.Select118();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select119()
    {
      var q = theTests.Select119();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select120()
    {
      var q = theTests.Select120();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select121()
    {
      var q = theTests.Select121();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select122()
    {
      var q = theTests.Select122();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select123()
    {
      var q = theTests.Select123();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select124()
    {
      var q = theTests.Select124();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select125()
    {
      var q = theTests.Select125();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select126()
    {
      var q = theTests.Select126();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select127()
    {
      var q = theTests.Select127();
    }

    [Benchmark]
    [BenchmarkCategory("Selects")]
    public void Select128()
    {
      var q = theTests.Select128();
    }

    #endregion

    //[Benchmark]
    //[BenchmarkCategory("Inserts")]
    //public void Insert01()
    //{
    //  var q = theTests.Insert01();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Updates")]
    //public void Update01()
    //{
    //  var q = theTests.Update01();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Updates")]
    //public void Update02()
    //{
    //  var q = theTests.Update02();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Updates")]
    //public void Update03()
    //{
    //  var q = theTests.Update03();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Updates")]
    //public void Update04()
    //{
    //  var q = theTests.Update04();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Updates")]
    //public void Update05()
    //{
    //  var q = theTests.Update05();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Updates")]
    //public void Update06()
    //{
    //  var q = theTests.Update06();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Updates")]
    //public void Update07()
    //{
    //  var q = theTests.Update07();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Updates")]
    //public void Update08()
    //{
    //  var q = theTests.Update08();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Updates")]
    //public void Update09()
    //{
    //  var q = theTests.Update09();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Updates")]
    //public void Update10()
    //{
    //  var q = theTests.Update10();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Updates")]
    //public void Update11()
    //{
    //  var q = theTests.Update11();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Updates")]
    //public void Update12()
    //{
    //  var q = theTests.Update12();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Deletes")]
    //public void Delete01()
    //{
    //  var q = theTests.Delete01();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Deletes")]
    //public void Delete02()
    //{
    //  var q = theTests.Delete02();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Deletes")]
    //public void Delete03()
    //{
    //  var q = theTests.Delete03();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Deletes")]
    //public void Delete04()
    //{
    //  var q = theTests.Delete04();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Deletes")]
    //public void Delete05()
    //{
    //  var q = theTests.Delete05();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Variables")]
    //public void Variable01()
    //{
    //  theTests.Variable01();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Variables")]
    //public void Variable02()
    //{
    //  theTests.Variable02();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Variables")]
    //public void Cursor01()
    //{
    //  theTests.Cursor01();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Cursors")]
    //public void Cursor02()
    //{
    //  theTests.Cursor02();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Cursors")]
    //public void Cursor03()
    //{
    //  theTests.Cursor03();
    //}

    //#region Drops

    //[Benchmark]
    //[BenchmarkCategory("Drops")]
    //public void DropTable01()
    //{
    //  theTests.DropTable01();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Drops")]
    //public void DropTable02()
    //{
    //  theTests.DropTable02();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Drops")]
    //public void DropSchema01()
    //{
    //  theTests.DropSchema01();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Drops")]
    //public void DropSchema02()
    //{
    //  theTests.DropTable02();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Drops")]
    //public void DropSequence01()
    //{
    //  theTests.DropSequence01();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Drops")]
    //public void DropPartitionFunctuon()
    //{
    //  theTests.DropPartitionFunctuon();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Drops")]
    //public void DropPartitionSchema()
    //{
    //  theTests.DropPartitionSchema();
    //}

    //#endregion

    //#region Creates

    //[Benchmark]
    //[BenchmarkCategory("Creates")]
    //public void CreateTable01()
    //{
    //  theTests.CreateTable01();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Creates")]
    //public void CreateTable02()
    //{
    //  theTests.CreateTable02();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Creates")]
    //public void CreateTableWithPartitionSchema1()
    //{
    //  theTests.CreateTableWithPartitionSchema1();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Creates")]
    //public void CreateTableWithPartitionDescriptor1()
    //{
    //  theTests.CreateTableWithPartitionDescriptor1();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Creates")]
    //public void CreateTableWithPartitionDescriptor2()
    //{
    //  theTests.CreateTableWithPartitionDescriptor2();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Creates")]
    //public void CreateTableWithPartitionDescriptor3()
    //{
    //  theTests.CreateTableWithPartitionDescriptor3();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Creates")]
    //public void CreateTableWithPartitionDescriptor4()
    //{
    //  theTests.CreateTableWithPartitionDescriptor4();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Creates")]
    //public void CreateAssertion01()
    //{
    //  theTests.CreateAssertion01();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Creates")]
    //public void CreateDomain01()
    //{
    //  theTests.CreateDomain01();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Creates")]
    //public void CreateSchema01()
    //{
    //  theTests.CreateSchema01();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Creates")]
    //public void CreateSequence01()
    //{
    //  theTests.CreateSequence01();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Creates")]
    //public void CreateSequence02()
    //{
    //  theTests.CreateSequence02();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Creates")]
    //public void CreatePartitionFunction()
    //{
    //  theTests.CreatePartitionFunction();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Creates")]
    //public void CreatePartitionSchema01()
    //{
    //  theTests.CreatePartitionSchema01();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Creates")]
    //public void CreatePartitionSchema02()
    //{
    //  theTests.CreatePartitionSchema02();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Creates")]
    //public void CreateIndex()
    //{
    //  theTests.CreateIndex();
    //}

    //#endregion

    //#region Alters

    //[Benchmark]
    //[BenchmarkCategory("Alters")]
    //public void AlterTable01()
    //{
    //  theTests.AlterTable01();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Alters")]
    //public void AlterTable02()
    //{
    //  theTests.AlterTable02();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Alters")]
    //public void AlterTable03()
    //{
    //  theTests.AlterTable03();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Alters")]
    //public void AlterTable04()
    //{
    //  theTests.AlterTable04();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Alters")]
    //public void AlterTable05()
    //{
    //  theTests.AlterTable05();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Alters")]
    //public void AlterTable06()
    //{
    //  theTests.AlterTable06();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Alters")]
    //public void AlterPartitionFunction()
    //{
    //  theTests.AlterPartitionFunction();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Alters")]
    //public void AlterPartitionSchema01()
    //{
    //  theTests.AlterPartitionSchema01();
    //}

    //[Benchmark]
    //[BenchmarkCategory("Alters")]
    //public void AlterPartitionSchema02()
    //{
    //  theTests.AlterPartitionSchema02();
    //}

    //#endregion


    public MSSQLQueriesBenchmarks()
    {
      theTests = new();
    }
  }
}
