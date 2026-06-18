namespace SharpSocketIO.EngineIo.Commons;

/// <summary>Port of AttachOptions.</summary>
public sealed class AttachOptions
{
    public string Path { get; set; } = "/engine.io";
    public bool DestroyUpgrade { get; set; } = true;
    public int DestroyUpgradeTimeout { get; set; } = 1000;
    public bool AddTrailingSlash { get; set; } = true;
}
