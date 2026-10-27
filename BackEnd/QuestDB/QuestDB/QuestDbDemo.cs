using System.Net;
using System.Net.Sockets;

namespace QuestDB.test
{
    public class QuestDbDemo : IAsyncLifetime
    {
        private const string _questDbHost = "localhost";
        private const int _questDbPort = 9000;
        private readonly TimeSpan _delay = TimeSpan.FromSeconds(30);
        private string Repository => "quest_integration_test";
        private string ConfString => $"ws::addr={_questDbHost}:{_questDbPort};";

        private string GenerateIndexName(string prefix) => 
            $"{prefix}_{DateTime.Now:yyyyMMddHH}";


        [Fact]
        public async Task A_Ingestion_streaming()
        {
            var timestamp = DateTime.Now.SetUtcSecondPrecision();
            
            using var timeout = new CancellationTokenSource(_delay);

            await using var sender = Sender.New(ConfString);
            await sender.Table(Repository)
                .Symbol("index_name", GenerateIndexName("streaming"))
                .Column("value", 2.0)
                .Column("range", new double[,] { { 1.0 }, { 3.0 } })
                .Column("version", timestamp)
                .AtAsync(timestamp, timeout.Token);

            await sender.Table(Repository)
                .Symbol("index_name", GenerateIndexName("streaming"))
                .Column("value", 666.666)
                .Column("range", new double[,] { { 555.555 }, { 777.777 } })
                .Column("version", timestamp)
                .AtAsync(timestamp.AddSeconds(1), timeout.Token);

            await sender.SendAsync(timeout.Token);
        }

        /// <summary>
        /// For the current QuestDbPoint struct (88 bytes) and the max QuestDB batch size (16 MB)
        /// the maximum number of rows should not exceed 150 000.
        /// <seealso cref="https://questdb.com/docs/connect/wire-protocols/qwp-ingress-websocket/?utm_source=kapa#protocol-limits"/>
        [Fact]
        public async Task B_Ingestion_batch()
        {
            var dataFrame = Data.GenerateDataFrame(150);
            const int batchSize = 150_000; // ~16 MB / 88 bytes

            var batchCount = (dataFrame.Length + batchSize - 1) / batchSize;

            using var timeout = new CancellationTokenSource(_delay);
            await using var client = QuestDBClient.Connect(ConfString);
            var options = new ParallelOptions
            {
                MaxDegreeOfParallelism = 4, // Default max concurrent batches
                CancellationToken = timeout.Token
            };

            await Parallel.ForEachAsync(
                Enumerable.Range(0, batchCount),
                options,
                async (batchIndex, cancellationToken) =>
                {
                    var offset = batchIndex * batchSize;
                    var count = Math.Min(batchSize, dataFrame.Length - offset);
                    var batch = new ArraySegment<QuestDbPoint>(dataFrame, offset, count);

                    using var sender = await client.BorrowSenderAsync(cancellationToken);
                    foreach (var point in batch)
                    {
                        sender.Table(Repository)
                            .Symbol("index_name", GenerateIndexName("bulk"))
                            .Column("value", point.Value)
                            .Column("range", point.Range)
                            .Column("version", point.Version)
                            .At(point.Timestamp, cancellationToken);
                    }

                    await sender.SendAsync(cancellationToken);
                });
        }

        [Fact]
        public async Task C_Query_simple()
        {
            var result = new List<(DateTime Timestamp, double Value)>();
            await using var client = QuestDBClient.Connect(ConfString);
            await using var reader = await client.NewQuery()
                .Sql($"SELECT time, value FROM {Repository} WHERE index_name = $1 AND time >= $2 AND time <= $3;")
                .Binds(x => { 
                    x.SetVarchar(0, GenerateIndexName("bulk")); 
                    x.SetTimestampMicros(1, new DateTime(2000, 1, 1).ConvertToTimestampMicros()); 
                    x.SetTimestampMicros(2, new DateTime(2024, 12, 31, 23, 59, 59).ConvertToTimestampMicros()); })
                .ExecuteReaderAsync();

            while (await reader.ReadBatchAsync())
            {
                var batch = reader.Current; //Batching 16 384 rows
                for (var row = 0; row < batch.RowCount; row++)
                {
                    result.Add((batch.GetTimestampValue(0, row).ConvertToDateTime(), batch.GetDoubleValue(1, row)));
                }
            }
            
            Assert.NotEmpty(result);
        }

        [Fact]
        public async Task D_Query_with_resampling_and_aggregating()
        {
            var result = new List<(DateTime Timestamp, double Avg, double Min, double Max)>();
            await using var client = QuestDBClient.Connect(ConfString);
            await using var reader = await client.NewQuery()
                .Sql($"SELECT time, avg(value) as avg_value, min(value) as min_value, max(value) as max_value FROM {Repository} WHERE index_name = $1 AND time >= $2 AND time <= $3 SAMPLE BY 1d;")
                .Binds(x => {
                    x.SetVarchar(0, GenerateIndexName("bulk"));
                    x.SetTimestampMicros(1, new DateTime(2000, 1, 1).ConvertToTimestampMicros());
                    x.SetTimestampMicros(2, new DateTime(2024, 12, 31, 23, 59, 59).ConvertToTimestampMicros());
                })
                .ExecuteReaderAsync();

            while (await reader.ReadBatchAsync())
            {
                var batch = reader.Current; //Batching 16 384 rows
                for (var row = 0; row < batch.RowCount; row++)
                {
                    result.Add((batch.GetTimestampValue(0, row).ConvertToDateTime(), batch.GetDoubleValue(1, row), batch.GetDoubleValue(2, row), batch.GetDoubleValue(3, row)));
                }
            }

            Assert.NotEmpty(result);
        }

        public async Task InitializeAsync()
        {
            await CheckIfQuestDbIsRunning();
            await RepoInitialisation();
        }

        private static async Task CheckIfQuestDbIsRunning()
        {
            using var client = new TcpClient();
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(3));
            try
            {
                await client.ConnectAsync(IPAddress.Loopback, _questDbPort, timeout.Token);
            }
            catch (SocketException ex)
            {
                throw new InvalidOperationException($"Cannot connect to QuestDB on {_questDbHost}:{_questDbPort}.", ex);
            }
            catch (OperationCanceledException ex)
            {
                throw new TimeoutException($"Connecting to QuestDB on {_questDbHost}:{_questDbPort} timed out.", ex);
            }
        }

        private async Task RepoInitialisation()
        {
            await using var client = await QueryClient.NewAsync(ConfString);
            // No 'ExecuteNonQuery' available!
            await using var reader = await client.ExecuteReaderAsync(
                $"CREATE TABLE IF NOT EXISTS {Repository} (index_name SYMBOL INDEX, version TIMESTAMP, time TIMESTAMP, value DOUBLE, range DOUBLE[][]) TIMESTAMP(time) PARTITION BY MONTH TTL 100 YEARS DEDUP UPSERT KEYS (index_name, version, time);"
            );
        }

        public Task DisposeAsync() => Task.CompletedTask;
    }
}