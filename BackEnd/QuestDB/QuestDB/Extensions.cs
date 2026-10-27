namespace QuestDB.test
{
    internal static class Extensions
    {
        public static long ConvertToTimestampMicros(this DateTime datetime) =>
            new DateTimeOffset(DateTime.SpecifyKind(datetime, DateTimeKind.Utc)).ToUnixTimeMilliseconds() * 1000;

        public static DateTime ConvertToDateTime(this long unixMicroseconds) =>
            DateTimeOffset.FromUnixTimeMilliseconds(unixMicroseconds / 1000).UtcDateTime;

        public static DateTime SetUtcSecondPrecision(this DateTime datetime) =>
            new(datetime.Ticks - datetime.Ticks % TimeSpan.TicksPerSecond, DateTimeKind.Utc);
    }
}
