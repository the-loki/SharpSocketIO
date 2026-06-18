using SharpSocketIO.EngineIo.Contrib;
using Xunit;

namespace SharpSocketIO.EngineIo.Tests;

public class Base64IdTests
{
    [Fact]
    public void GenerateId_returns_url_safe_base64_of_20_chars()
    {
        var id = Base64Id.GenerateId();
        Assert.Equal(20, id.Length);
        Assert.Matches("^[A-Za-z0-9_-]+$", id);
    }

    [Fact]
    public void GenerateId_produces_distinct_values()
    {
        var ids = new System.Collections.Generic.HashSet<string>();
        for (int i = 0; i < 1000; i++) Assert.True(ids.Add(Base64Id.GenerateId()));
    }
}
