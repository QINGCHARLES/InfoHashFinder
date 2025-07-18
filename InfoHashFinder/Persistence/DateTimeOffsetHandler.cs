using System.Data;
using System.Globalization;
using Dapper;

namespace InfoHashFinder.Persistence;

/// <summary>
/// Forces Dapper to persist <see cref="DateTimeOffset"/> as ISO-8601 UTC text
/// and parse it back reliably from TEXT values returned by SQLite.
/// </summary>
internal sealed class DateTimeOffsetHandler : SqlMapper.TypeHandler<DateTimeOffset>
{
	private const string Format = "O"; // Round-trip ISO 8601

	public override void SetValue(IDbDataParameter Parameter, DateTimeOffset Value)
	{
		// Always store in UTC; TEXT column.
		Parameter.DbType = DbType.String;
		Parameter.Value = Value.ToUniversalTime().ToString(Format, CultureInfo.InvariantCulture);
	}

	public override DateTimeOffset Parse(object Value)
	{
		if (Value is null)
		{
			return DateTimeOffset.MinValue;
		}

		// SQLite provider tends to give us string; occasionally DateTime.
		if (Value is string S)
		{
			// Try exact round-trip first, fallback to general parse.
			if (DateTimeOffset.TryParseExact(
					S,
					Format,
					CultureInfo.InvariantCulture,
					DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
					out DateTimeOffset Dto))
			{
				return Dto;
			}

			// Generic parse fallback.
			return DateTimeOffset.Parse(
				S,
				CultureInfo.InvariantCulture,
				DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
		}

		if (Value is DateTime Dt)
		{
			// Assume the provider gave us UTC DateTime.
			return new DateTimeOffset(DateTime.SpecifyKind(Dt, DateTimeKind.Utc));
		}

		// Last-ditch cast (will throw if incompatible)
		return (DateTimeOffset)Value;
	}
}
