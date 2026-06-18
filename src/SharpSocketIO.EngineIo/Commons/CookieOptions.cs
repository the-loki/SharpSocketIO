namespace SharpSocketIO.EngineIo.Commons;

/// <summary>Port of CookieSerializeOptions (from the 'cookie' npm package).</summary>
public sealed class CookieOptions
{
    public int? MaxAge { get; set; }
    public DateTime? Expires { get; set; }
    public string? Path { get; set; }
    public string? Domain { get; set; }
    public bool? Secure { get; set; }
    public bool? HttpOnly { get; set; }
    public bool? SameSite { get; set; }
    public bool? Signed { get; set; }
    public bool? Overwrite { get; set; }
}
