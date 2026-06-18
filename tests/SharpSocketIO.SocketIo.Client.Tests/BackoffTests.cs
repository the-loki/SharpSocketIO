using global::SharpSocketIO.SocketIoClient.Contrib;
using Xunit;

namespace SharpSocketIO.Tests.SocketIoClient;

public class BackoffTests
{
    [Fact]
    public void Duration_grows_exponentially_without_jitter()
    {
        var b = new Backoff(new BackoffOptions { Min = 100, Max = 10000, Factor = 2, Jitter = 0 });
        Assert.Equal(100, b.Duration());   // 100 * 2^0
        Assert.Equal(200, b.Duration());   // 100 * 2^1
        Assert.Equal(400, b.Duration());   // 100 * 2^2
        Assert.Equal(800, b.Duration());   // 100 * 2^3
    }

    [Fact]
    public void Duration_is_capped_at_max()
    {
        var b = new Backoff(new BackoffOptions { Min = 1000, Max = 5000, Factor = 2, Jitter = 0 });
        Assert.Equal(1000, b.Duration());
        Assert.Equal(2000, b.Duration());
        Assert.Equal(4000, b.Duration());
        Assert.Equal(5000, b.Duration()); // would be 8000, capped
        Assert.Equal(5000, b.Duration()); // would be 16000, capped
    }

    [Fact]
    public void Reset_sets_attempts_to_zero()
    {
        var b = new Backoff(new BackoffOptions { Jitter = 0 });
        b.Duration(); b.Duration(); b.Duration();
        Assert.Equal(3, b.Attempts);
        b.Reset();
        Assert.Equal(0, b.Attempts);
        Assert.Equal(100, b.Duration()); // back to 100 * 2^0
    }

    [Fact]
    public void Jitter_keeps_duration_within_bounds()
    {
        var b = new Backoff(new BackoffOptions { Min = 1000, Max = 100000, Factor = 2, Jitter = 0.5 });
        // after 1 attempt, base = 2000; with 0.5 jitter the result is in [1000, 3000]
        for (int i = 0; i < 50; i++)
        {
            b.Reset();
            b.Duration(); // attempt 0 → base 1000
            var d = b.Duration(); // attempt 1 → base 2000 ± jitter
            Assert.InRange(d, 1000, 3000);
        }
    }
}
