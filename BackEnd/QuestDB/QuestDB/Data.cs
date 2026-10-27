namespace QuestDB.test
{
    public readonly record struct QuestDbPoint(DateTime Timestamp, double Value, double[,] Range, DateTime Version);

    internal class Data
    {
        public static QuestDbPoint[] GenerateDataFrame(int years)
        {
            const long intervalTicks = 15L * TimeSpan.TicksPerMinute;
            int count = (int)(TimeSpan.FromDays(365 * years).Ticks / intervalTicks);
            var points = new QuestDbPoint[count];
            var timestampTicks = DateTime.UnixEpoch.Ticks;
            var version = DateTime.Now.SetUtcSecondPrecision();

            for (var i = 0; i < points.Length; i++)
            {
                points[i] = new QuestDbPoint(
                    Timestamp: new DateTime(timestampTicks, DateTimeKind.Utc),
                    Value: i * 0.9,
                    Range: new double[,] { { i - 1.0 }, { i + 1.0 } },
                    Version: version);

                timestampTicks += intervalTicks;
            }

            return points;
        }
    }
}
