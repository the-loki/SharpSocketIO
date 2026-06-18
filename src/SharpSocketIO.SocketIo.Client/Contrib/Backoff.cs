using System;

namespace SharpSocketIO.SocketIo.Client.Contrib;

/// <summary>
/// Port of lib/contrib/backo2.ts. Exponential backoff with optional jitter for reconnect.
/// </summary>
public sealed class Backoff
{
    public int Ms { get; private set; }
    public int Max { get; private set; }
    public int Factor { get; }
    public double Jitter { get; private set; }
    public int Attempts { get; private set; }

    public Backoff(BackoffOptions? opts = null)
    {
        opts ??= new BackoffOptions();
        Ms = opts.Min ?? 100;
        Max = opts.Max ?? 10000;
        Factor = opts.Factor ?? 2;
        Jitter = (opts.Jitter is { } j && j > 0 && j <= 1) ? j : 0;
        Attempts = 0;
    }

    /// <summary>Returns the backoff duration in ms and increments the attempt count.</summary>
    public int Duration()
    {
        var ms = Ms * Math.Pow(Factor, Attempts++);
        if (Jitter > 0)
        {
            var rand = Random.Shared.NextDouble();
            var deviation = Math.Floor(rand * Jitter * ms);
            ms = (Math.Floor(rand * 10) % 2) == 0 ? ms - deviation : ms + deviation;
        }
        return (int)Math.Min(ms, Max);
    }

    public void Reset() => Attempts = 0;
    public void SetMin(int min) => Ms = min;
    public void SetMax(int max) => Max = max;
    public void SetJitter(double jitter) => Jitter = jitter;
}

public sealed class BackoffOptions
{
    public int? Min { get; set; }
    public int? Max { get; set; }
    public int? Factor { get; set; }
    public double? Jitter { get; set; }
}
